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

package org.apache.druid.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.segment.column.RowSignature;

public class PhysicalDatasourceMetadata
{
  private final TableDataSource dataSource;
  private final RowSignature rowSignature;

  private final boolean joinable;
  private final boolean broadcast;

  @JsonCreator
  public PhysicalDatasourceMetadata(
      @JsonProperty("dataSource") final TableDataSource dataSource,
      @JsonProperty("rowSignature") final RowSignature rowSignature,
      @JsonProperty("isJoinable") final boolean isJoinable,
      @JsonProperty("isBroadcast") final boolean isBroadcast
  )
  {
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.rowSignature = Preconditions.checkNotNull(rowSignature, "rowSignature");
    this.joinable = isJoinable;
    this.broadcast = isBroadcast;
  }

  @JsonProperty
  public TableDataSource dataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public RowSignature rowSignature()
  {
    return rowSignature;
  }

  @JsonProperty
  public boolean isJoinable()
  {
    return joinable;
  }

  @JsonProperty
  public boolean isBroadcast()
  {
    return broadcast;
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

    PhysicalDatasourceMetadata that = (PhysicalDatasourceMetadata) o;

    if (!Objects.equal(dataSource, that.dataSource)) {
      return false;
    }
    return Objects.equal(rowSignature, that.rowSignature);
  }

  @Override
  public int hashCode()
  {
    int result = dataSource != null ? dataSource.hashCode() : 0;
    result = 31 * result + (rowSignature != null ? rowSignature.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "DatasourceMetadata{" +
           "dataSource=" + dataSource +
           ", rowSignature=" + rowSignature +
           '}';
  }
}
