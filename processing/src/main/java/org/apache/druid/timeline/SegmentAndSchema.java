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

import org.apache.druid.segment.column.SegmentSchema;

import java.util.Objects;

/**
 *
 */
public class SegmentAndSchema
{
  private final DataSegment dataSegment;
  private final SegmentSchema segmentSchema;

  public SegmentAndSchema(DataSegment dataSegment, SegmentSchema segmentSchema)
  {
    this.dataSegment = dataSegment;
    this.segmentSchema = segmentSchema;
  }

  public DataSegment getDataSegment()
  {
    return dataSegment;
  }

  public SegmentSchema getSegmentSchema()
  {
    return segmentSchema;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SegmentAndSchema that = (SegmentAndSchema) o;
    return Objects.equals(dataSegment, that.dataSegment) && Objects.equals(
        segmentSchema,
        that.segmentSchema
    );
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dataSegment, segmentSchema);
  }
}
