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

package org.apache.druid.server.http;

import com.google.api.client.util.Sets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.segment.metadata.AvailableSegmentMetadata;
import org.apache.druid.segment.metadata.SegmentMetadataCache;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentStatusInCluster;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MetadataResourceTest
{
  private static final String DATASOURCE1 = "datasource1";

  private final DataSegment[] segments =
      CreateDataSegments.ofDatasource(DATASOURCE1)
                        .forIntervals(3, Granularities.DAY)
                        .withNumPartitions(2)
                        .eachOfSizeInMb(500)
                        .toArray(new DataSegment[0]);
  private HttpServletRequest request;
  private SegmentsMetadataManager segmentsMetadataManager;
  private IndexerMetadataStorageCoordinator storageCoordinator;
  private DruidCoordinator coordinator;


  private MetadataResource metadataResource;

  @Before
  public void setUp()
  {
    request = Mockito.mock(HttpServletRequest.class);
    Mockito.doReturn(Mockito.mock(AuthenticationResult.class))
           .when(request).getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT);

    segmentsMetadataManager = Mockito.mock(SegmentsMetadataManager.class);
    ImmutableDruidDataSource druidDataSource1 = new ImmutableDruidDataSource(
        DATASOURCE1,
        ImmutableMap.of(),
        ImmutableList.of(segments[0], segments[1], segments[2], segments[3])
    );

    DataSourcesSnapshot dataSourcesSnapshot = Mockito.mock(DataSourcesSnapshot.class);
    Mockito.doReturn(dataSourcesSnapshot)
           .when(segmentsMetadataManager).getSnapshotOfDataSourcesWithAllUsedSegments();
    Mockito.doReturn(ImmutableList.of(druidDataSource1))
           .when(dataSourcesSnapshot).getDataSourcesWithAllUsedSegments();
    Mockito.doReturn(druidDataSource1)
           .when(segmentsMetadataManager)
           .getImmutableDataSourceWithUsedSegments(DATASOURCE1);

    coordinator = Mockito.mock(DruidCoordinator.class);
    Mockito.doReturn(2).when(coordinator).getReplicationFactor(segments[0].getId());
    Mockito.doReturn(null).when(coordinator).getReplicationFactor(segments[1].getId());
    Mockito.doReturn(1).when(coordinator).getReplicationFactor(segments[2].getId());
    Mockito.doReturn(1).when(coordinator).getReplicationFactor(segments[3].getId());
    Mockito.doReturn(ImmutableSet.of(segments[3]))
           .when(dataSourcesSnapshot).getOvershadowedSegments();

    storageCoordinator = Mockito.mock(IndexerMetadataStorageCoordinator.class);
    Mockito.doReturn(segments[4])
           .when(storageCoordinator)
           .retrieveUsedSegmentForId(segments[4].getId().toString());
    Mockito.doReturn(null)
           .when(storageCoordinator)
           .retrieveUsedSegmentForId(segments[5].getId().toString());

    metadataResource = new MetadataResource(
        segmentsMetadataManager,
        storageCoordinator,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        coordinator,
        null
    );
  }

  @Test
  public void testGetAllSegmentsWithOvershadowedStatus()
  {
    Response response = metadataResource.getAllUsedSegments(request, null, "includeOvershadowedStatus", null);

    final List<SegmentStatusInCluster> resultList = extractSegmentStatusList(response);
    Assert.assertEquals(resultList.size(), 4);
    Assert.assertEquals(new SegmentStatusInCluster(segments[0], false, 2, null, false), resultList.get(0));
    Assert.assertEquals(new SegmentStatusInCluster(segments[1], false, null,  null, false), resultList.get(1));
    Assert.assertEquals(new SegmentStatusInCluster(segments[2], false, 1, null, false), resultList.get(2));
    // Replication factor should be 0 as the segment is overshadowed
    Assert.assertEquals(new SegmentStatusInCluster(segments[3], true, 0,  null, false), resultList.get(3));
  }

  @Test
  public void testGetAllSegmentsIncludingRealtime()
  {
    SegmentMetadataCache segmentMetadataCache = Mockito.mock(SegmentMetadataCache.class);

    String dataSource2 = "datasource2";

    DataSegment[] realTimeSegments =
        CreateDataSegments.ofDatasource(dataSource2)
                          .forIntervals(3, Granularities.DAY)
                          .withNumPartitions(2)
                          .eachOfSizeInMb(500)
                          .toArray(new DataSegment[0]);

    Mockito.doReturn(null).when(coordinator).getReplicationFactor(realTimeSegments[0].getId());
    Mockito.doReturn(null).when(coordinator).getReplicationFactor(realTimeSegments[1].getId());
    Map<SegmentId, AvailableSegmentMetadata> availableSegments = new HashMap<>();
    availableSegments.put(
        segments[0].getId(),
        AvailableSegmentMetadata.builder(
            segments[0],
            0L,
            Sets.newHashSet(),
            null,
            20L
        ).build()
    );
    availableSegments.put(
        segments[1].getId(),
        AvailableSegmentMetadata.builder(
            segments[1],
            0L,
            Sets.newHashSet(),
            null,
            30L
        ).build()
    );
    availableSegments.put(
        segments[1].getId(),
        AvailableSegmentMetadata.builder(
            segments[1],
            0L,
            Sets.newHashSet(),
            null,
            30L
        ).build()
    );
    availableSegments.put(
        realTimeSegments[0].getId(),
        AvailableSegmentMetadata.builder(
            realTimeSegments[0],
            1L,
            Sets.newHashSet(),
            null,
            10L
        ).build()
    );
    availableSegments.put(
        realTimeSegments[1].getId(),
        AvailableSegmentMetadata.builder(
            realTimeSegments[1],
            1L,
            Sets.newHashSet(),
            null,
            40L
        ).build()
    );

    Mockito.doReturn(availableSegments).when(segmentMetadataCache).getSegmentMetadataSnapshot();

    Mockito.doReturn(availableSegments.get(segments[0].getId()))
           .when(segmentMetadataCache)
           .getAvailableSegmentMetadata(DATASOURCE1, segments[0].getId());

    Mockito.doReturn(availableSegments.get(segments[1].getId()))
           .when(segmentMetadataCache)
           .getAvailableSegmentMetadata(DATASOURCE1, segments[1].getId());

    metadataResource = new MetadataResource(
        segmentsMetadataManager,
        storageCoordinator,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        coordinator,
        segmentMetadataCache
    );

    Response response = metadataResource.getAllUsedSegments(request, null, "includeOvershadowedStatus", "includeRealtimeSegments");

    final List<SegmentStatusInCluster> resultList = extractSegmentStatusList(response);
    Assert.assertEquals(resultList.size(), 6);
    Assert.assertEquals(new SegmentStatusInCluster(segments[0], false, 2, 20L, false), resultList.get(0));
    Assert.assertEquals(new SegmentStatusInCluster(segments[1], false, null,  30L, false), resultList.get(1));
    Assert.assertEquals(new SegmentStatusInCluster(segments[2], false, 1, null, false), resultList.get(2));
    // Replication factor should be 0 as the segment is overshadowed
    Assert.assertEquals(new SegmentStatusInCluster(segments[3], true, 0,  null, false), resultList.get(3));
    Assert.assertEquals(new SegmentStatusInCluster(realTimeSegments[0], false, null, 10L, true), resultList.get(4));
    Assert.assertEquals(new SegmentStatusInCluster(realTimeSegments[1], false, null,  40L, true), resultList.get(5));
  }

  @Test
  public void testGetUsedSegment()
  {
    // Available in snapshot
    Assert.assertEquals(
        segments[0],
        metadataResource.getUsedSegment(segments[0].getDataSource(), segments[0].getId().toString()).getEntity()
    );

    // Unavailable in snapshot, but available in metadata
    Assert.assertEquals(
        segments[4],
        metadataResource.getUsedSegment(segments[4].getDataSource(), segments[4].getId().toString()).getEntity()
    );

    // Unavailable in both snapshot and metadata
    Assert.assertNull(
        metadataResource.getUsedSegment(segments[5].getDataSource(), segments[5].getId().toString()).getEntity()
    );
  }

  private List<SegmentStatusInCluster> extractSegmentStatusList(Response response)
  {
    return Lists.newArrayList(
        (Iterable<SegmentStatusInCluster>) response.getEntity()
    );
  }
}
