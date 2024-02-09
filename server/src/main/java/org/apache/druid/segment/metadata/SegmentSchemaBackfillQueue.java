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

package org.apache.druid.segment.metadata;

import com.google.inject.Inject;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.SchemaPayload;
import org.apache.druid.segment.column.SegmentSchemaMetadata;
import org.apache.druid.segment.metadata.SchemaManager.SegmentSchemaMetadataPlus;
import org.apache.druid.timeline.SegmentId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@ManageLifecycle
public class SegmentSchemaBackfillQueue
{
  private static final EmittingLogger log = new EmittingLogger(SegmentSchemaBackfillQueue.class);
  private static final int MAX_BATCH_SIZE = 500;
  private final BlockingDeque<SegmentSchemaMetadataPlus> queue = new LinkedBlockingDeque<>();
  private final long executionPeriod;
  private final ScheduledExecutorService executor;

  private final SegmentSchemaCache segmentSchemaCache;
  private final SchemaManager schemaManager;
  private final SchemaFingerprintGenerator schemaFingerprintGenerator;

  @Inject
  public SegmentSchemaBackfillQueue(
      SchemaManager schemaManager,
      ScheduledExecutorFactory scheduledExecutorFactory,
      SegmentSchemaCache segmentSchemaCache,
      SchemaFingerprintGenerator schemaFingerprintGenerator
  )
  {
    this.schemaManager = schemaManager;
    this.executor = scheduledExecutorFactory.create(1, "SegmentSchemaBackfillQueue-%s");
    this.segmentSchemaCache = segmentSchemaCache;
    this.executionPeriod = TimeUnit.MINUTES.toMillis(1);
    this.schemaFingerprintGenerator = schemaFingerprintGenerator;
  }

  @LifecycleStart
  public void start()
  {
    if (isEnabled()) {
      scheduleQueuePoll(executionPeriod);
    }
  }

  @LifecycleStop
  public void stop()
  {
    if (isEnabled()) {
      executor.shutdownNow();
    }
  }

  public void add(SegmentId segmentId, RowSignature rowSignature, long numRows, Map<String, AggregatorFactory> aggregators)
  {
    SchemaPayload schemaPayload = new SchemaPayload(rowSignature, aggregators);
    SegmentSchemaMetadata schemaMetadata = new SegmentSchemaMetadata(schemaPayload, numRows);
    queue.add(new SegmentSchemaMetadataPlus(segmentId, schemaMetadata, schemaFingerprintGenerator.generateId(schemaMetadata.getSchemaPayload())));
  }

  private void add(SegmentSchemaMetadataPlus plus)
  {
    queue.add(new SegmentSchemaMetadataPlus(plus.getSegmentId(), plus.getSegmentSchemaMetadata(), plus.getFingerprint()));
  }

  public boolean isEnabled()
  {
    return executor != null && !executor.isShutdown();
  }

  private void scheduleQueuePoll(long delay)
  {
    executor.schedule(this::processBatchesDue, delay, TimeUnit.MILLISECONDS);
  }

  private void processBatchesDue()
  {
    int itemsToProcess = Math.min(MAX_BATCH_SIZE, queue.size());

    List<SegmentSchemaMetadataPlus> polled = new ArrayList<>();

    for (int i = 0; i < itemsToProcess; i++) {
      SegmentSchemaMetadataPlus item = queue.poll();
      if (item != null) {
        polled.add(item);
      }
    }

    try {
      schemaManager.persistSchemaAndUpdateSegmentsTable(polled);
    }
    catch (Exception e) {
      // implies that the transaction failed
      polled.forEach(this::add);
    }
    finally {
      for (SegmentSchemaMetadataPlus plus : polled) {
        segmentSchemaCache.markInTransitSMQResultPublished(plus.getSegmentId());
      }
    }
  }
}
