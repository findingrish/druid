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

package org.apache.druid.grpc.server;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.SqlRowTransformer;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.table.RowSignatures;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.DateTimeZone;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Optional;
import java.util.TimeZone;

public class ProtobufTransformer
{

  @Nullable
  public static Object transform(SqlRowTransformer rowTransformer, Object[] row, int i)
  {
    final RelDataType rowType = rowTransformer.getRowType();
    final SqlTypeName sqlTypeName = rowType.getFieldList().get(i).getType().getSqlTypeName();
    final RowSignature signature = RowSignatures.fromRelDataType(rowType.getFieldNames(), rowType);
    final Optional<ColumnType> columnType = signature.getColumnType(i);

    if (sqlTypeName == SqlTypeName.TIMESTAMP
            || sqlTypeName == SqlTypeName.DATE) {
      final DateTime dateTime;
      if (sqlTypeName == SqlTypeName.TIMESTAMP) {
        dateTime = Calcites.calciteTimestampToJoda((long) row[i], DateTimeZone.forTimeZone(TimeZone.getTimeZone("UTC")));
      } else {
        dateTime = Calcites.calciteDateToJoda((int) row[i], DateTimeZone.forTimeZone(TimeZone.getTimeZone("UTC")));
      }
      long seconds = DateTimeUtils.getInstantMillis(dateTime) / 1000;
      return Timestamp.newBuilder().setSeconds(seconds).build();
    }

    if (!columnType.isPresent()) {
      return convertComplexType(row[i]);
    }

    final ColumnType druidType = columnType.get();

    if (druidType == ColumnType.STRING) {
      return row[i];
    } else if (druidType == ColumnType.LONG) {
      return row[i];
    } else if (druidType == ColumnType.FLOAT) {
      return row[i];
    } else if (druidType == ColumnType.DOUBLE) {
      return row[i];
    } else {
      return convertComplexType(row[i]);
    }
  }

  private static ByteString convertComplexType(Object value)
  {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
         ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      oos.writeObject(value);
      oos.flush();
      return ByteString.copyFrom(bos.toByteArray());
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
