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

package org.apache.druid.sql.calcite.schema;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.segment.metadata.SegmentMetadataCacheConfig;
import org.joda.time.Period;

public class BrokerSegmentMetadataCacheConfig extends SegmentMetadataCacheConfig
{
  @JsonProperty
  private boolean metadataSegmentCacheEnable = true;

  @JsonProperty
  private long metadataSegmentPollPeriod = 60000;

  @JsonProperty
  private boolean awaitInitializationOnStart = true;

  public static BrokerSegmentMetadataCacheConfig create()
  {
    return new BrokerSegmentMetadataCacheConfig();
  }

  public static BrokerSegmentMetadataCacheConfig create(
      String metadataRefreshPeriod
  )
  {
    BrokerSegmentMetadataCacheConfig config = new BrokerSegmentMetadataCacheConfig();
    config.setMetadataRefreshPeriod(new Period(metadataRefreshPeriod));
    return config;
  }

  public boolean isMetadataSegmentCacheEnable()
  {
    return metadataSegmentCacheEnable;
  }

  public long getMetadataSegmentPollPeriod()
  {
    return metadataSegmentPollPeriod;
  }

  @Override
  public boolean isAwaitInitializationOnStart()
  {
    return awaitInitializationOnStart;
  }
}
