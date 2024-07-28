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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.metadata.SqlSegmentsMetadataManager;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentStatusInCluster;
import org.apache.druid.timeline.SegmentTimeline;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An immutable snapshot of metadata information about used segments and overshadowed segments, coming from
 * {@link SqlSegmentsMetadataManager}.
 */
public class DataSourcesSnapshot
{
  public static DataSourcesSnapshot fromUsedSegments(
      Iterable<DataSegment> segments,
      ImmutableMap<String, String> dataSourceProperties
  )
  {
    return fromUsedSegments(
        segments,
        dataSourceProperties,
        ImmutableMap.of()
    );
  }

  public static DataSourcesSnapshot fromUsedSegments(
      Iterable<DataSegment> segments,
      ImmutableMap<String, String> dataSourceProperties,
      Map<String, Set<SegmentId>> loadedSegmentsPerDataSource
  )
  {
    Map<String, DruidDataSource> dataSources = new HashMap<>();
    segments.forEach(
        segment ->
            dataSources.computeIfAbsent(
                segment.getDataSource(),
                dsName -> new DruidDataSource(dsName, dataSourceProperties)
            ).addSegmentIfAbsent(segment)
    );

    Map<String, ImmutableDruidDataSource> immutableDruidDataSources = CollectionUtils.mapValues(
        dataSources,
        DruidDataSource::toImmutableDruidDataSource
    );
    Map<String, SegmentTimeline> usedSegmentsTimelinesPerDataSource = CollectionUtils.mapValues(
        immutableDruidDataSources,
        dataSource -> SegmentTimeline.forSegments(dataSource.getSegments())
    );
    ImmutableSet<DataSegment> overshadowedSegments = determineOvershadowedSegments(
        immutableDruidDataSources,
        usedSegmentsTimelinesPerDataSource
    );
    return new DataSourcesSnapshot(
        immutableDruidDataSources,
        usedSegmentsTimelinesPerDataSource,
        overshadowedSegments,
        loadedSegmentsPerDataSource
    );
  }

  private final Map<String, ImmutableDruidDataSource> dataSourcesWithAllUsedSegments;
  private final Map<String, SegmentTimeline> usedSegmentsTimelinesPerDataSource;
  private final ImmutableSet<DataSegment> overshadowedSegments;
  private final Map<String, Set<SegmentId>> loadedSegmentsPerDataSource;

  public DataSourcesSnapshot(
      Map<String, ImmutableDruidDataSource> dataSourcesWithAllUsedSegments
  )
  {
    this(
        dataSourcesWithAllUsedSegments,
        CollectionUtils.mapValues(
            dataSourcesWithAllUsedSegments,
            dataSource -> SegmentTimeline.forSegments(dataSource.getSegments())
        ),
        ImmutableMap.of()
    );
  }

  private DataSourcesSnapshot(
      Map<String, ImmutableDruidDataSource> dataSourcesWithAllUsedSegments,
      Map<String, SegmentTimeline> usedSegmentsTimelinesPerDataSource,
      Map<String, Set<SegmentId>> loadedSegmentsPerDataSource
  )
  {
    this(
        dataSourcesWithAllUsedSegments,
        usedSegmentsTimelinesPerDataSource,
        determineOvershadowedSegments(dataSourcesWithAllUsedSegments, usedSegmentsTimelinesPerDataSource),
        loadedSegmentsPerDataSource
    );
  }

  private DataSourcesSnapshot(
      Map<String, ImmutableDruidDataSource> dataSourcesWithAllUsedSegments,
      Map<String, SegmentTimeline> usedSegmentsTimelinesPerDataSource,
      ImmutableSet<DataSegment> overshadowedSegments,
      Map<String, Set<SegmentId>> loadedSegmentsPerDataSource
  )
  {
    this.dataSourcesWithAllUsedSegments = dataSourcesWithAllUsedSegments;
    this.usedSegmentsTimelinesPerDataSource = usedSegmentsTimelinesPerDataSource;
    this.overshadowedSegments = overshadowedSegments;
    this.loadedSegmentsPerDataSource = loadedSegmentsPerDataSource;
  }

  public Collection<ImmutableDruidDataSource> getDataSourcesWithAllUsedSegments()
  {
    return dataSourcesWithAllUsedSegments.values();
  }

  public Map<String, ImmutableDruidDataSource> getDataSourcesMap()
  {
    return dataSourcesWithAllUsedSegments;
  }

  @Nullable
  public ImmutableDruidDataSource getDataSource(String dataSourceName)
  {
    return dataSourcesWithAllUsedSegments.get(dataSourceName);
  }

  public Map<String, SegmentTimeline> getUsedSegmentsTimelinesPerDataSource()
  {
    return usedSegmentsTimelinesPerDataSource;
  }

  public ImmutableSet<DataSegment> getOvershadowedSegments()
  {
    return overshadowedSegments;
  }

  public Map<String, Set<SegmentId>> getLoadedSegmentsPerDataSource()
  {
    return loadedSegmentsPerDataSource;
  }

  /**
   * Returns an iterable to go over all used segments in all data sources. The order in which segments are iterated
   * is unspecified.
   *
   * Note: the iteration may not be as trivially cheap as, for example, iteration over an ArrayList. Try (to some
   * reasonable extent) to organize the code so that it iterates the returned iterable only once rather than several
   * times.
   *
   * This method's name starts with "iterate" because the result is expected to be consumed immediately in a for-each
   * statement or a stream pipeline, like
   * for (DataSegment segment : snapshot.iterateAllUsedSegmentsInSnapshot()) {...}
   */
  public Iterable<DataSegment> iterateAllUsedSegmentsInSnapshot()
  {
    return () -> dataSourcesWithAllUsedSegments
        .values()
        .stream()
        .flatMap(dataSource -> dataSource.getSegments().stream())
        .iterator();
  }

  /**
   * This method builds timelines from all data sources and finds the overshadowed segments list
   *
   * This method should be deduplicated with {@link VersionedIntervalTimeline#findFullyOvershadowed()}: see
   * https://github.com/apache/druid/issues/8070.
   *
   * @return List of overshadowed segments
   */
  private static ImmutableSet<DataSegment> determineOvershadowedSegments(
      Map<String, ImmutableDruidDataSource> dataSourcesWithAllUsedSegments,
      Map<String, SegmentTimeline> usedSegmentsTimelinesPerDataSource)
  {
    // It's fine to add all overshadowed segments to a single collection because only
    // a small fraction of the segments in the cluster are expected to be overshadowed,
    // so building this collection shouldn't generate a lot of garbage.
    final List<DataSegment> overshadowedSegments = new ArrayList<>();
    for (ImmutableDruidDataSource dataSource : dataSourcesWithAllUsedSegments.values()) {
      SegmentTimeline usedSegmentsTimeline =
          usedSegmentsTimelinesPerDataSource.get(dataSource.getName());
      for (DataSegment segment : dataSource.getSegments()) {
        if (usedSegmentsTimeline.isOvershadowed(segment)) {
          overshadowedSegments.add(segment);
        }
      }
    }
    return ImmutableSet.copyOf(overshadowedSegments);
  }

  /**
   * Note that the returned set of segments doesn't contain numRows information
   */
  public static Set<SegmentStatusInCluster> getSegmentsWithOvershadowedAndLoadStatus(
      Collection<ImmutableDruidDataSource> segments,
      Set<DataSegment> overshadowedSegments,
      Map<String, Set<SegmentId>> loadedSegmentsPerDataSource
  )
  {
    final Stream<DataSegment> usedSegments = segments
        .stream()
        .flatMap(t -> t.getSegments().stream());

    return usedSegments
        .map(segment ->
                 new SegmentStatusInCluster(
                     segment,
                     overshadowedSegments.contains(segment),
                     null,
                     null,
                     false,
                     getLoadStatusForSegment(
                         loadedSegmentsPerDataSource,
                         segment.getDataSource(),
                         segment.getId()
                     )
                 )
        )
        .collect(Collectors.toSet());
  }

  private static boolean getLoadStatusForSegment(
      Map<String, Set<SegmentId>> loadedSegmentsPerDataSource,
      String dataSource,
      SegmentId segmentId
  )
  {
    return loadedSegmentsPerDataSource.containsKey(dataSource)
           && loadedSegmentsPerDataSource.get(dataSource).contains(segmentId);
  }
}
