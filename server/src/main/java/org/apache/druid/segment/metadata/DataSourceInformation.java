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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.segment.column.RowSignature;

import java.util.Objects;

/**
 * Encapsulates schema information of a dataSource.
 */
public class DataSourceInformation
{
  private final String datasource;
  private final RowSignature rowSignature;

  @JsonCreator
  public DataSourceInformation(
      @JsonProperty("datasource") String datasource,
      @JsonProperty("rowSignature") RowSignature rowSignature)
  {
    this.datasource = datasource;
    this.rowSignature = rowSignature;
  }

  @JsonProperty
  public String getDatasource()
  {
    return datasource;
  }

  @JsonProperty
  public RowSignature getRowSignature()
  {
    return rowSignature;
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
    DataSourceInformation that = (DataSourceInformation) o;
    return Objects.equals(datasource, that.datasource) && Objects.equals(
        rowSignature,
        that.rowSignature
    );
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(datasource, rowSignature);
  }

  @Override
  public String toString()
  {
    return "DataSourceSchema{" +
           "datasource='" + datasource + '\'' +
           ", rowSignature=" + rowSignature +
           '}';
  }
}
