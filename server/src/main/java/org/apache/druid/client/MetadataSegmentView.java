/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.inject.Inject;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.server.coordination.ChangeRequestHistory;
import org.apache.druid.server.coordination.ChangeRequestsSnapshot;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.DataSegmentChange;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentStatusInCluster;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * This class polls the Coordinator in background to keep the latest published segments.
 * Provides {@link #getPublishedSegments()} for others to get segments in metadata store.
 *
 * The difference between this class and {@link SegmentsMetadataManager} is that this class resides
 * in Broker's memory, while {@link SegmentsMetadataManager} resides in Coordinator's memory. In
 * fact, this class polls the data from {@link SegmentsMetadataManager} object in the memory of the
 * currently leading Coordinator via HTTP queries.
 */
@ManageLifecycle
public class MetadataSegmentView
{
  private static final EmittingLogger log = new EmittingLogger(MetadataSegmentView.class);

  private final DruidLeaderClient coordinatorDruidLeaderClient;
  private final ObjectMapper jsonMapper;
  private final BrokerSegmentWatcherConfig segmentWatcherConfig;

  private final boolean isCacheEnabled;
  private final boolean isDetectUnavailableSegmentsEnabled;

  /**
   * Use {@link ImmutableSortedSet} so that the order of segments is deterministic and
   * sys.segments queries return the segments in sorted order based on segmentId.
   *
   * Volatile since this reference is reassigned in {@code poll()} or {@code pollChangedSegments()} and then read in {@code getPublishedSegments()}
   * from other threads.
   */
  @MonotonicNonNull
  private volatile ImmutableSortedSet<SegmentStatusInCluster> publishedSegments = null;
  /**
   * Caches the replication factor for segment IDs. In case of coordinator restarts or leadership re-elections, the coordinator API returns `null` replication factor until load rules are evaluated.
   * The cache can be used during these periods to continue serving the previously fetched values.
   */
  private final Cache<SegmentId, Integer> segmentIdToReplicationFactor;
  private final ScheduledExecutorService scheduledExec;
  private final long pollPeriodInMS;
  private final LifecycleLock lifecycleLock = new LifecycleLock();
  private final CountDownLatch cachePopulated = new CountDownLatch(1);

  @Nullable
  private ChangeRequestHistory.Counter counter = null;

  private final ConcurrentMap<ServerView.HandedOffSegmentCallback, Executor> segmentCallbacks = new ConcurrentHashMap<>();

  private final CountDownLatch initializationLatch = new CountDownLatch(1);

  @Inject
  public MetadataSegmentView(
      final @Coordinator DruidLeaderClient druidLeaderClient,
      final ObjectMapper jsonMapper,
      final BrokerSegmentWatcherConfig segmentWatcherConfig,
      final SegmentMetadataCacheConfig config
  )
  {
    Preconditions.checkNotNull(config, "SegmentMetadataCacheConfig");
    this.coordinatorDruidLeaderClient = druidLeaderClient;
    this.jsonMapper = jsonMapper;
    this.segmentWatcherConfig = segmentWatcherConfig;
    this.isCacheEnabled = config.isMetadataSegmentCacheEnable();
    this.pollPeriodInMS = config.getMetadataSegmentPollPeriod();
    this.scheduledExec = Execs.scheduledSingleThreaded("MetadataSegmentView-Cache--%d");
    this.segmentIdToReplicationFactor = CacheBuilder.newBuilder()
                                                    .expireAfterAccess(10, TimeUnit.MINUTES)
                                                    .build();
    this.isDetectUnavailableSegmentsEnabled = segmentWatcherConfig.isDetectUnavailableSegments();
  }

  @LifecycleStart
  public void start()
  {
    if (!lifecycleLock.canStart()) {
      throw new ISE("can't start.");
    }
    try {
      if (isCacheEnabled || isDetectUnavailableSegmentsEnabled) {
        scheduledExec.schedule(new PollTask(), pollPeriodInMS, TimeUnit.MILLISECONDS);
      }
      lifecycleLock.started();
      log.info("MetadataSegmentView Started.");
    }
    finally {
      lifecycleLock.exitStart();
    }
  }

  @LifecycleStop
  public void stop()
  {
    if (!lifecycleLock.canStop()) {
      throw new ISE("can't stop.");
    }
    log.info("MetadataSegmentView is stopping.");
    if (isCacheEnabled) {
      scheduledExec.shutdown();
    }
    log.info("MetadataSegmentView Stopped.");
  }

  private void poll()
  {
    log.info("polling published segments from coordinator");
    final JsonParserIterator<SegmentStatusInCluster> metadataSegments = getMetadataSegments(
        coordinatorDruidLeaderClient,
        jsonMapper,
        segmentWatcherConfig.getWatchedDataSources()
    );

    final ImmutableSortedSet.Builder<SegmentStatusInCluster> builder = ImmutableSortedSet.naturalOrder();
    while (metadataSegments.hasNext()) {
      final SegmentStatusInCluster segmentStatusInCluster = convert(metadataSegments.next());
      builder.add(segmentStatusInCluster);
    }

    publishedSegments = builder.build();
    cachePopulated.countDown();
  }

  protected void pollChangedSegments()
  {
    log.debug("polling changed segments from coordinator");
    final ChangeRequestsSnapshot<DataSegmentChange> changedRequestsSnapshot = getChangedSegments(
        coordinatorDruidLeaderClient,
        jsonMapper,
        segmentWatcherConfig.getWatchedDataSources()
    );

    if (null == changedRequestsSnapshot) {
      log.error("ChangedRequestsSnapshot object polled from coordinator is null");
      return;
    }

    if (null == changedRequestsSnapshot.getRequests()) {
      log.error("change requests list in ChangedRequestsSnapshot is null");
      return;
    }

    Map<SegmentId, SegmentStatusInCluster> publishedSegmentsCopy = new HashMap<>();

    final List<DataSegmentChange> dataSegmentChanges =
        changedRequestsSnapshot
            .getRequests()
            .stream()
            .map(dataSegmentChange ->
                     new DataSegmentChange(
                         convert(dataSegmentChange.getSegmentStatusInCluster()),
                         dataSegmentChange.getChangeType()
                     ))
            .collect(Collectors.toList());

    counter = changedRequestsSnapshot.getCounter();

    log.debug("counter [%d], hash [%d], segments changed [%d], full sync: [%s]",
              counter.getCounter(),
              counter.getHash(),
              dataSegmentChanges.size(),
              changedRequestsSnapshot.isResetCounter()
    );

    log.debug("Changes [%s]", dataSegmentChanges);

    if (changedRequestsSnapshot.isResetCounter()) {
      runSegmentCallbacks(
          input -> input.fullSync(dataSegmentChanges)
      );

      if (isCacheEnabled) {
        dataSegmentChanges.forEach(
            dataSegmentChange -> publishedSegmentsCopy.put(
                dataSegmentChange.getSegmentStatusInCluster().getDataSegment().getId(),
                dataSegmentChange.getSegmentStatusInCluster()
            ));
      }
    } else {
      runSegmentCallbacks(
          input -> input.deltaSync(dataSegmentChanges)
      );

      if (isCacheEnabled) {
        publishedSegments.stream().iterator().forEachRemaining(
            segment -> publishedSegmentsCopy.put(segment.getDataSegment().getId(), segment));
        dataSegmentChanges.forEach(dataSegmentChange -> {
          if (dataSegmentChange.isLoad()) {
            publishedSegmentsCopy.put(
                dataSegmentChange.getSegmentStatusInCluster().getDataSegment().getId(),
                dataSegmentChange.getSegmentStatusInCluster()
            );
          } else {
            publishedSegmentsCopy.remove(dataSegmentChange.getSegmentStatusInCluster().getDataSegment().getId());
          }
        });
      }
    }

    if (initializationLatch.getCount() > 0) {
      initializationLatch.countDown();
      log.info("synced segments metadata successfully for the first time.");
      runSegmentCallbacks(ServerView.HandedOffSegmentCallback::segmentViewInitialized);
    }

    if (isCacheEnabled) {
      ImmutableSortedSet.Builder<SegmentStatusInCluster> builder = ImmutableSortedSet.naturalOrder();
      builder.addAll(publishedSegmentsCopy.values());
      publishedSegments = builder.build();
      cachePopulated.countDown();
    }
  }

  private SegmentStatusInCluster convert(SegmentStatusInCluster segment)
  {
    final DataSegment interned = DataSegmentInterner.intern(segment.getDataSegment());
    Integer replicationFactor = segment.getReplicationFactor();
    if (replicationFactor == null) {
      replicationFactor = segmentIdToReplicationFactor.getIfPresent(segment.getDataSegment().getId());
    } else {
      segmentIdToReplicationFactor.put(segment.getDataSegment().getId(), segment.getReplicationFactor());
    }
    return new SegmentStatusInCluster(
        interned,
        segment.isOvershadowed(),
        replicationFactor,
        segment.isHandedOff()
    );
  }

  public void registerSegmentCallback(
      Executor exec,
      ServerView.HandedOffSegmentCallback segmentCallback
  )
  {
    segmentCallbacks.put(segmentCallback, exec);
  }

  private void runSegmentCallbacks(
      final Consumer<ServerView.HandedOffSegmentCallback> fn
  )
  {
    for (final Map.Entry<ServerView.HandedOffSegmentCallback, Executor> entry : segmentCallbacks.entrySet()) {
      entry.getValue().execute(
          () -> fn.accept(entry.getKey())
      );
    }
  }

  public Iterator<SegmentStatusInCluster> getPublishedSegments()
  {
    if (isCacheEnabled) {
      Uninterruptibles.awaitUninterruptibly(cachePopulated);
      return publishedSegments.iterator();
    } else {
      return getMetadataSegments(
          coordinatorDruidLeaderClient,
          jsonMapper,
          segmentWatcherConfig.getWatchedDataSources()
      );
    }
  }

  // Note that coordinator must be up to get segments
  private JsonParserIterator<SegmentStatusInCluster> getMetadataSegments(
      DruidLeaderClient coordinatorClient,
      ObjectMapper jsonMapper,
      Set<String> watchedDataSources
  )
  {
    String query = "/druid/coordinator/v1/metadata/segments?includeOvershadowedStatus";
    if (watchedDataSources != null && !watchedDataSources.isEmpty()) {
      log.debug(
          "filtering datasources in published segments based on broker's watchedDataSources[%s]", watchedDataSources);
      final StringBuilder sb = new StringBuilder();
      for (String ds : watchedDataSources) {
        sb.append("datasources=").append(ds).append("&");
      }
      sb.setLength(sb.length() - 1);
      query = "/druid/coordinator/v1/metadata/segments?includeOvershadowedStatus&" + sb;
    }

    return coordinatorClient.getThingsFromLeaderNode(
        query,
        new TypeReference<SegmentStatusInCluster>()
        {
        },
        jsonMapper
    );
  }

  @Nullable
  private ChangeRequestsSnapshot<DataSegmentChange> getChangedSegments(
      DruidLeaderClient coordinatorClient,
      ObjectMapper jsonMapper,
      Set<String> watchedDataSources
  )
  {
    StringBuilder queryBuilder = new StringBuilder();
    queryBuilder.append("/druid/coordinator/v1/metadata/changedSegments?");

    if (watchedDataSources != null && !watchedDataSources.isEmpty()) {
      log.debug(
          "filtering datasources in published segments based on broker's watchedDataSources[%s]", watchedDataSources);
      for (String ds : watchedDataSources) {
        queryBuilder.append("datasources=").append(ds).append("&");
      }
    }

    if (null == counter) {
      queryBuilder.append("counter=-1");
    } else {
      queryBuilder.append(StringUtils.format("counter=%s&hash=%s", counter.getCounter(), counter.getHash()));
    }

    ChangeRequestsSnapshot<DataSegmentChange> changeRequestsSnapshot;

    try {
      changeRequestsSnapshot = jsonMapper.readValue(
          coordinatorClient.getThingsFromLeaderNode(queryBuilder.toString()).rhs,
          new TypeReference<ChangeRequestsSnapshot<DataSegmentChange>>() {});
    }
    catch (IOException e) {
      throw new ISE(e, "Unable to parse ChangeRequestSnapshot.");
    }

    return changeRequestsSnapshot;
  }

  private class PollTask implements Runnable
  {
    @Override
    public void run()
    {
      long delayMS = pollPeriodInMS;
      try {
        final long pollStartTime = System.nanoTime();
        if (isDetectUnavailableSegmentsEnabled) {
          pollChangedSegments();
        } else {
          poll();
        }
        final long pollEndTime = System.nanoTime();
        final long pollTimeNS = pollEndTime - pollStartTime;
        final long pollTimeMS = TimeUnit.NANOSECONDS.toMillis(pollTimeNS);
        delayMS = Math.max(pollPeriodInMS - pollTimeMS, 0);
      }
      catch (Exception e) {
        log.makeAlert(e, "Problem polling Coordinator.").emit();
      }
      finally {
        if (!Thread.currentThread().isInterrupted()) {
          scheduledExec.schedule(new PollTask(), delayMS, TimeUnit.MILLISECONDS);
        }
      }
    }
  }

}