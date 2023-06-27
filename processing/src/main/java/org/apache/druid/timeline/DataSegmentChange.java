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

package org.apache.druid.timeline;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.druid.java.util.common.StringUtils;

public class DataSegmentChange
{
  private final SegmentStatusInCluster segmentStatusInCluster;
  private final ChangeType changeType;

  @JsonCreator
  public DataSegmentChange(
      @JsonProperty("segmentStatusInCluster") SegmentStatusInCluster segmentStatusInCluster,
      @JsonProperty("changeType") ChangeType changeType
  )
  {
    this.segmentStatusInCluster = segmentStatusInCluster;
    this.changeType = changeType;
  }

  @JsonProperty
  public SegmentStatusInCluster getSegmentStatusInCluster()
  {
    return segmentStatusInCluster;
  }

  @JsonProperty
  public ChangeType getChangeType()
  {
    return changeType;
  }

  @JsonIgnore
  public boolean isLoad()
  {
    return changeType != ChangeType.SEGMENT_REMOVED;
  }

  @Override
  public String toString()
  {
    return "DataSegmentChangeRequest{" +
           ", changeReason=" + changeType +
           ", segmentStatusInCluster=" + segmentStatusInCluster +
           '}';
  }

  public enum ChangeType
  {
    SEGMENT_ADDED,
    SEGMENT_REMOVED,
    SEGMENT_OVERSHADOWED,
    SEGMENT_HANDED_OFF,
    SEGMENT_OVERSHADOWED_AND_HANDED_OFF;

    @JsonValue
    @Override
    public String toString()
    {
      return StringUtils.toLowerCase(this.name());
    }

    @JsonCreator
    public static ChangeType fromString(String name)
    {
      return valueOf(StringUtils.toUpperCase(name));
    }
  }
}