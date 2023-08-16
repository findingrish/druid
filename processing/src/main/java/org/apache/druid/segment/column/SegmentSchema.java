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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.util.Objects;

/**
 *
 */
public class SegmentSchema
{
  private static final HashFunction HASH_FUNCTION = Hashing.sha256();
  private final RowSignature rowSignature;
  private final String fingerprint;

  @JsonCreator
  public SegmentSchema(
      @JsonProperty("rowSignature") RowSignature rowSignature,
      @JsonProperty("fingerPrint") String fingerPrint
  )
  {
    this.rowSignature = rowSignature;
    this.fingerprint = fingerPrint;
  }

  public SegmentSchema(RowSignature rowSignature)
  {
    this.rowSignature = rowSignature;
    this.fingerprint = String.valueOf(rowSignature.hashCode()); // todo compute sha256
  }

  public RowSignature getRowSignature()
  {
    return rowSignature;
  }

  public String getFingerprint()
  {
    return fingerprint;
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
    SegmentSchema that = (SegmentSchema) o;
    return Objects.equals(rowSignature, that.rowSignature) && Objects.equals(
        fingerprint,
        that.fingerprint
    );
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(rowSignature, fingerprint);
  }
}
