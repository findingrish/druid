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

package org.apache.druid.segment.column;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.aggregation.last.StringLastAggregatorFactory;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

public class SegmentSchemaMetadataTest
{

  static {
    NullHandling.initializeForTests();
  }

  @Test
  public void testSerde() throws IOException
  {
    RowSignature rowSignature = RowSignature.builder().add("c", ColumnType.FLOAT).build();

    StringLastAggregatorFactory factory = new StringLastAggregatorFactory("billy", "nilly", null, 20);
    SchemaPayload payload = new SchemaPayload(rowSignature, Collections.singletonMap("twosum", factory));

    SegmentSchemaMetadata metadata = new SegmentSchemaMetadata(payload, 20L);

    ObjectMapper mapper = TestHelper.makeJsonMapper();
    byte[] bytes = mapper.writeValueAsBytes(metadata);
    SegmentSchemaMetadata deserialized = mapper.readValue(bytes, SegmentSchemaMetadata.class);

    Assert.assertEquals(metadata, deserialized);
  }
}
