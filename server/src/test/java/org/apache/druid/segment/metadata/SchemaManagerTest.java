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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.first.LongFirstAggregatorFactory;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.SchemaPayload;
import org.apache.druid.segment.column.SegmentSchemaMetadata;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.skife.jdbi.v2.Update;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class SchemaManagerTest
{
  static {
    NullHandling.initializeForTests();
  }

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private final ObjectMapper mapper = TestHelper.makeJsonMapper();

  SchemaManager schemaManager;

  TestDerbyConnector derbyConnector;
  MetadataStorageTablesConfig tablesConfig;
  SchemaFingerprintGenerator fingerprintGenerator;
  SegmentSchemaTestUtils segmentSchemaTestUtils;

  @Before
  public void setUp()
  {
    derbyConnector = derbyConnectorRule.getConnector();
    tablesConfig = derbyConnectorRule.metadataTablesConfigSupplier().get();

    derbyConnector.createSegmentSchemaTable();
    derbyConnector.createSegmentTable();

    schemaManager = new SchemaManager(derbyConnectorRule.metadataTablesConfigSupplier().get(), mapper, derbyConnector);
    segmentSchemaTestUtils = new SegmentSchemaTestUtils(derbyConnectorRule, derbyConnector, mapper);
    fingerprintGenerator = new SchemaFingerprintGenerator(mapper);
  }

  @Test
  public void testPersistSchemaAndUpdateSegmentsTable()
  {
    final Map<String, Pair<SchemaPayload, Integer>> segmentIdSchemaMap = new HashMap<>();
    Random random = new Random(5);

    Set<DataSegment> segments = new HashSet<>();
    List<SchemaManager.SegmentSchemaMetadataPlus> schemaMetadataPluses = new ArrayList<>();

    for (int i = 1; i < 9; i++) {
      final DataSegment segment = new DataSegment(
          "foo",
          Intervals.of("2023-01-0" + i + "/2023-01-0" + (i + 1)),
          "2023-01-0" + i,
          ImmutableMap.of("path", "a-" + i),
          ImmutableList.of("dim1"),
          ImmutableList.of("m1"),
          new LinearShardSpec(0),
          9,
          100
      );

      segments.add(segment);

      int randomNum = random.nextInt();
      RowSignature rowSignature = RowSignature.builder().add("c" + randomNum, ColumnType.FLOAT).build();
      Map<String, AggregatorFactory> aggregatorFactoryMap = new HashMap<>();
      aggregatorFactoryMap.put("longFirst", new LongFirstAggregatorFactory("longFirst", "long-col", null));

      SchemaPayload schemaPayload = new SchemaPayload(rowSignature, aggregatorFactoryMap);
      SegmentSchemaMetadata schemaMetadata = new SegmentSchemaMetadata(schemaPayload, (long) randomNum);
      SchemaManager.SegmentSchemaMetadataPlus plus =
          new SchemaManager.SegmentSchemaMetadataPlus(
              segment.getId(),
              schemaMetadata,
              fingerprintGenerator.generateId(schemaPayload)
          );
      schemaMetadataPluses.add(plus);
      segmentIdSchemaMap.put(segment.getId().toString(), Pair.of(schemaPayload, randomNum));
    }

    segmentSchemaTestUtils.insertUsedSegments(segments, Collections.emptyMap());
    schemaManager.persistSchemaAndUpdateSegmentsTable(schemaMetadataPluses);

    segmentSchemaTestUtils.verifySegmentSchema(segmentIdSchemaMap);
  }

  @Test
  public void testCleanUpUnreferencedSchema()
  {
    final Map<String, Pair<SchemaPayload, Integer>> segmentIdSchemaMap = new HashMap<>();
    Random random = new Random(5);

    Set<DataSegment> segments = new HashSet<>();
    List<SchemaManager.SegmentSchemaMetadataPlus> schemaMetadataPluses = new ArrayList<>();

    for (int i = 1; i < 9; i++) {
      final DataSegment segment = new DataSegment(
          "foo",
          Intervals.of("2023-01-0" + i + "/2023-01-0" + (i + 1)),
          "2023-01-0" + i,
          ImmutableMap.of("path", "a-" + i),
          ImmutableList.of("dim1"),
          ImmutableList.of("m1"),
          new LinearShardSpec(0),
          9,
          100
      );

      segments.add(segment);

      int randomNum = random.nextInt();
      RowSignature rowSignature = RowSignature.builder().add("c" + randomNum, ColumnType.FLOAT).build();

      SchemaPayload schemaPayload = new SchemaPayload(rowSignature);
      SegmentSchemaMetadata schemaMetadata = new SegmentSchemaMetadata(schemaPayload, (long) randomNum);
      SchemaManager.SegmentSchemaMetadataPlus plus =
          new SchemaManager.SegmentSchemaMetadataPlus(
              segment.getId(),
              schemaMetadata,
              fingerprintGenerator.generateId(schemaPayload)
          );
      schemaMetadataPluses.add(plus);
      segmentIdSchemaMap.put(segment.getId().toString(), Pair.of(schemaPayload, randomNum));
    }

    segmentSchemaTestUtils.insertUsedSegments(segments, Collections.emptyMap());
    schemaManager.persistSchemaAndUpdateSegmentsTable(schemaMetadataPluses);

    segmentSchemaTestUtils.verifySegmentSchema(segmentIdSchemaMap);

    derbyConnector.retryWithHandle(handle -> {
      Update deleteStatement = handle.createStatement(StringUtils.format("DELETE FROM %s", tablesConfig.getSegmentsTable()));
      deleteStatement.execute();
      return true;
    });

    schemaManager.cleanUpUnreferencedSchema();

    List<Long> ids = derbyConnector.retryWithHandle(
        handle -> handle.createQuery(StringUtils.format(
                            "SELECT id from %s",
                            tablesConfig.getSegmentSchemaTable()
                        ))
                        .mapTo(Long.class)
                        .list());

    Assert.assertTrue(ids.isEmpty());
  }
}
