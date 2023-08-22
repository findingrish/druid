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
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.GlobalTableDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.Types;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.timeline.SegmentSchema;

import javax.annotation.Nullable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 *
 */
public class PhysicalDatasourceMetadataBuilder
{

  private final SegmentManager segmentManager;
  private final JoinableFactory joinableFactory;
  private final ColumnTypeMergePolicy columnTypeMergePolicy;

  @Inject
  public PhysicalDatasourceMetadataBuilder(SegmentManager segmentManager, JoinableFactory joinableFactory)
  {
    this.segmentManager = segmentManager;
    this.joinableFactory = joinableFactory;
    this.columnTypeMergePolicy = new LeastRestrictiveTypeMergePolicy(); // todo get from config SegmentMetadataCacheConfig.getColumnTypeMergePolicy
  }

  @VisibleForTesting
  @Nullable
  // todo move DatasourceTable to server package
  public PhysicalDatasourceMetadata buildDruidTable(String dataSource, final Set<SegmentSchema> schemaSet)
  {
    // Preserve order.
    final Map<String, ColumnType> columnTypes = new LinkedHashMap<>();

    for (SegmentSchema segmentSchema : schemaSet) {
      final RowSignature rowSignature = segmentSchema.getRowSignature();
      if (rowSignature != null) {
        for (String column : rowSignature.getColumnNames()) {
          final ColumnType columnType =
              rowSignature.getColumnType(column)
                          .orElseThrow(() -> new ISE("Encountered null type for column [%s]", column));

          columnTypes.compute(column, (c, existingType) -> columnTypeMergePolicy.merge(existingType, columnType));
        }
      }
    }
    final RowSignature.Builder builder = RowSignature.builder();
    columnTypes.forEach(builder::add);

    final TableDataSource tableDataSource;

    // to be a GlobalTableDataSource instead of a TableDataSource, it must appear on all servers (inferred by existing
    // in the segment cache, which in this case belongs to the broker meaning only broadcast segments live here)
    // to be joinable, it must be possibly joinable according to the factory. we only consider broadcast datasources
    // at this time, and isGlobal is currently strongly coupled with joinable, so only make a global table datasource
    // if also joinable
    final GlobalTableDataSource maybeGlobal = new GlobalTableDataSource(dataSource);
    final boolean isJoinable = joinableFactory.isDirectlyJoinable(maybeGlobal);
    final boolean isBroadcast = segmentManager.getDataSourceNames().contains(dataSource);
    if (isBroadcast && isJoinable) {
      tableDataSource = maybeGlobal;
    } else {
      tableDataSource = new TableDataSource(dataSource);
    }
    return new PhysicalDatasourceMetadata(tableDataSource, builder.build(), isJoinable, isBroadcast);
  }

  /**
   * ColumnTypeMergePolicy defines the rules of which type to use when faced with the possibility of different types
   * for the same column from segment to segment. It is used to help compute a {@link RowSignature} for a table in
   * Druid based on the segment metadata of all segments, merging the types of each column encountered to end up with
   * a single type to represent it globally.
   */
  @FunctionalInterface
  public interface ColumnTypeMergePolicy
  {
    ColumnType merge(ColumnType existingType, ColumnType newType);

    @JsonCreator
    static ColumnTypeMergePolicy fromString(String type)
    {
      if (LeastRestrictiveTypeMergePolicy.NAME.equalsIgnoreCase(type)) {
        return LeastRestrictiveTypeMergePolicy.INSTANCE;
      }
      if (FirstTypeMergePolicy.NAME.equalsIgnoreCase(type)) {
        return FirstTypeMergePolicy.INSTANCE;
      }
      throw new IAE("Unknown type [%s]", type);
    }
  }

  /**
   * Classic logic, we use the first type we encounter. This policy is effectively 'newest first' because we iterated
   * segments starting from the most recent time chunk, so this typically results in the most recently used type being
   * chosen, at least for systems that are continuously updated with 'current' data.
   *
   * Since {@link ColumnTypeMergePolicy} are used to compute the SQL schema, at least in systems using SQL schemas which
   * are partially or fully computed by this cache, this merge policy can result in query time errors if incompatible
   * types are mixed if the chosen type is more restrictive than the types of some segments. If data is likely to vary
   * in type across segments, consider using {@link LeastRestrictiveTypeMergePolicy} instead.
   */
  public static class FirstTypeMergePolicy implements ColumnTypeMergePolicy
  {
    public static final String NAME = "latestInterval";
    private static final FirstTypeMergePolicy INSTANCE = new FirstTypeMergePolicy();

    @Override
    public ColumnType merge(ColumnType existingType, ColumnType newType)
    {
      if (existingType == null) {
        return newType;
      }
      if (newType == null) {
        return existingType;
      }
      // if any are json, are all json
      if (ColumnType.NESTED_DATA.equals(newType) || ColumnType.NESTED_DATA.equals(existingType)) {
        return ColumnType.NESTED_DATA;
      }
      // "existing type" is the 'newest' type, since we iterate the segments list by newest start time
      return existingType;
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(NAME);
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      return o != null && getClass() == o.getClass();
    }

    @Override
    public String toString()
    {
      return NAME;
    }
  }

  /**
   * Resolves types using {@link ColumnType#leastRestrictiveType(ColumnType, ColumnType)} to find the ColumnType that
   * can best represent all data contained across all segments.
   */
  public static class LeastRestrictiveTypeMergePolicy implements ColumnTypeMergePolicy
  {
    public static final String NAME = "leastRestrictive";
    private static final LeastRestrictiveTypeMergePolicy INSTANCE = new LeastRestrictiveTypeMergePolicy();

    @Override
    public ColumnType merge(ColumnType existingType, ColumnType newType)
    {
      try {
        return ColumnType.leastRestrictiveType(existingType, newType);
      }
      catch (Types.IncompatibleTypeException incompatibleTypeException) {
        // fall back to first encountered type if they are not compatible for some reason
        return FirstTypeMergePolicy.INSTANCE.merge(existingType, newType);
      }
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(NAME);
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      return o != null && getClass() == o.getClass();
    }

    @Override
    public String toString()
    {
      return NAME;
    }
  }
}
