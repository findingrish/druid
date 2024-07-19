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

package org.apache.druid.frame.write;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import it.unimi.dsi.fastutil.objects.ObjectArrays;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.frame.key.ByteRowKeyComparatorTest;
import org.apache.druid.frame.key.KeyOrder;
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.nested.StructuredData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Data used by {@link FrameWriterTest}.
 */
public class FrameWriterTestData
{
  public static final Dataset<String> TEST_STRINGS_SINGLE_VALUE = new Dataset<>(
      ColumnType.STRING,
      Arrays.asList(
          null,
          NullHandling.emptyToNullIfNeeded(""), // Empty string in SQL-compatible mode, null otherwise
          "brown",
          "dog",
          "fox",
          "jumps",
          "lazy",
          "over",
          "quick",
          "the", // Repeated string
          "the",
          "thee", // To ensure "the" is before "thee"
          "\uD83D\uDE42",
          "\uD83E\uDD20",
          "\uD83E\uDEE5"
      )
  );

  /**
   * Single-value strings, mostly, but with an empty list thrown in.
   */
  public static final Dataset<Object> TEST_STRINGS_SINGLE_VALUE_WITH_EMPTY = new Dataset<>(
      ColumnType.STRING,
      Arrays.asList(
          Collections.emptyList(),
          NullHandling.emptyToNullIfNeeded(""), // Empty string in SQL-compatible mode, null otherwise
          "brown",
          "dog",
          "fox",
          "jumps",
          "lazy",
          "over",
          "quick",
          "the", // Repeated string
          "the",
          "thee", // To ensure "the" is before "thee"
          "\uD83D\uDE42",
          "\uD83E\uDD20",
          "\uD83E\uDEE5"
      )
  );

  /**
   * Multi-value strings in the form that {@link org.apache.druid.segment.DimensionSelector#getObject} would return
   * them: unwrapped string if single-element; list otherwise.
   */
  public static final Dataset<Object> TEST_STRINGS_MULTI_VALUE = new Dataset<>(
      ColumnType.STRING,
      Arrays.asList(
          Collections.emptyList(),
          null,
          NullHandling.emptyToNullIfNeeded(""),
          "foo",
          Arrays.asList("lazy", "dog"),
          "qux",
          Arrays.asList("the", "quick", "brown"),
          Arrays.asList("the", "quick", "brown", null),
          Arrays.asList("the", "quick", "brown", NullHandling.emptyToNullIfNeeded("")),
          Arrays.asList("the", "quick", "brown", "fox"),
          Arrays.asList("the", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog"),
          Arrays.asList("the", "quick", "brown", "null"),
          Arrays.asList("thee", "quick", "brown"),
          "\uD83D\uDE42",
          Arrays.asList("\uD83D\uDE42", "\uD83E\uDEE5"),
          "\uD83E\uDD20"
      )
  );

  /**
   * String arrays in the form that a {@link org.apache.druid.segment.ColumnValueSelector} would return them.
   */
  public static final Dataset<Object> TEST_ARRAYS_STRING = new Dataset<>(
      ColumnType.STRING_ARRAY,
      Arrays.asList(
          null,
          ObjectArrays.EMPTY_ARRAY,
          new Object[]{null},
          new Object[]{NullHandling.emptyToNullIfNeeded("")},
          new Object[]{"dog"},
          new Object[]{"lazy"},
          new Object[]{"the", "quick", "brown"},
          new Object[]{"the", "quick", "brown", null},
          new Object[]{"the", "quick", "brown", NullHandling.emptyToNullIfNeeded("")},
          new Object[]{"the", "quick", "brown", "fox"},
          new Object[]{"the", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog"},
          new Object[]{"the", "quick", "brown", "null"},
          new Object[]{"\uD83D\uDE42"},
          new Object[]{"\uD83D\uDE42", "\uD83E\uDEE5"}
      )
  );

  public static final Dataset<Long> TEST_LONGS = new Dataset<>(
      ColumnType.LONG,
      Stream.of(
          NullHandling.defaultLongValue(),
          0L,
          -1L,
          1L,
          Long.MAX_VALUE,
          Long.MIN_VALUE,
          101L,
          -101L,
          3L,
          -2L,
          2L,
          1L,
          -1L,
          -3L
      ).sorted(Comparators.naturalNullsFirst()).collect(Collectors.toList())
  );

  public static final Dataset<Object> TEST_ARRAYS_LONG = new Dataset<>(
      ColumnType.LONG_ARRAY,
      Arrays.asList(
          null,
          ObjectArrays.EMPTY_ARRAY,
          new Object[]{null},
          new Object[]{null, 6L, null, 5L, null},
          new Object[]{null, 6L, null, 5L, NullHandling.defaultLongValue()},
          new Object[]{null, 6L, null, 5L, 0L, -1L},
          new Object[]{null, 6L, null, 5L, 0L, -1L, Long.MIN_VALUE},
          new Object[]{null, 6L, null, 5L, 0L, -1L, Long.MAX_VALUE},
          new Object[]{5L},
          new Object[]{5L, 6L},
          new Object[]{5L, 6L, null},
          new Object[]{Long.MAX_VALUE, Long.MIN_VALUE}
      )
  );

  public static final Dataset<Float> TEST_FLOATS = new Dataset<>(
      ColumnType.FLOAT,
      Stream.of(
          0f,
          -0f,
          NullHandling.defaultFloatValue(),
          -1f,
          1f,
          //CHECKSTYLE.OFF: Regexp
          Float.MIN_VALUE,
          Float.MAX_VALUE,
          //CHECKSTYLE.ON: Regexp
          Float.NaN,
          -101f,
          101f,
          Float.POSITIVE_INFINITY,
          Float.NEGATIVE_INFINITY,
          0.004f,
          2.7e20f
      ).sorted(Comparators.naturalNullsFirst()).collect(Collectors.toList())
  );

  //CHECKSTYLE.OFF: Regexp
  public static final Dataset<Object> TEST_ARRAYS_FLOAT = new Dataset<>(
      ColumnType.FLOAT_ARRAY,
      Arrays.asList(
          null,
          ObjectArrays.EMPTY_ARRAY,
          new Object[]{null},
          new Object[]{null, 6.2f, null, 5.1f, null},
          new Object[]{null, 6.2f, null, 5.1f, NullHandling.defaultFloatValue()},
          new Object[]{null, 6.2f, null, 5.7f, 0.0f, -1.0f},
          new Object[]{null, 6.2f, null, 5.7f, 0.0f, -1.0f, Float.MIN_VALUE},
          new Object[]{null, 6.2f, null, 5.7f, 0.0f, -1.0f, Float.MAX_VALUE},
          new Object[]{Float.NEGATIVE_INFINITY, Float.MIN_VALUE},
          new Object[]{5.7f},
          new Object[]{5.7f, 6.2f},
          new Object[]{5.7f, 6.2f, null},
          new Object[]{Float.MAX_VALUE, Float.MIN_VALUE},
          new Object[]{Float.POSITIVE_INFINITY, Float.MIN_VALUE}
      )
  );
  //CHECKSTYLE.ON: Regexp

  public static final Dataset<Double> TEST_DOUBLES = new Dataset<>(
      ColumnType.DOUBLE,
      Stream.of(
          0d,
          -0d,
          NullHandling.defaultDoubleValue(),
          -1e-122d,
          1e122d,
          //CHECKSTYLE.OFF: Regexp
          Double.MIN_VALUE,
          Double.MAX_VALUE,
          //CHECKSTYLE.ON: Regexp
          Double.NaN,
          -101d,
          101d,
          Double.POSITIVE_INFINITY,
          Double.NEGATIVE_INFINITY,
          -0.000001d,
          2.7e100d
      ).sorted(Comparators.naturalNullsFirst()).collect(Collectors.toList())
  );

  //CHECKSTYLE.OFF: Regexp
  public static final Dataset<Object> TEST_ARRAYS_DOUBLE = new Dataset<>(
      ColumnType.DOUBLE_ARRAY,
      Arrays.asList(
          null,
          ObjectArrays.EMPTY_ARRAY,
          new Object[]{null},
          new Object[]{null, 6.2d, null, 5.1d, null},
          new Object[]{null, 6.2d, null, 5.1d, NullHandling.defaultDoubleValue()},
          new Object[]{null, 6.2d, null, 5.7d, 0.0d, -1.0d},
          new Object[]{null, 6.2d, null, 5.7d, 0.0d, -1.0d, Double.MIN_VALUE},
          new Object[]{null, 6.2d, null, 5.7d, 0.0d, -1.0d, Double.MAX_VALUE},
          new Object[]{Double.NEGATIVE_INFINITY, Double.MIN_VALUE},
          new Object[]{5.7d},
          new Object[]{5.7d, 6.2d},
          new Object[]{5.7d, 6.2d, null},
          new Object[]{Double.MAX_VALUE, Double.MIN_VALUE},
          new Object[]{Double.POSITIVE_INFINITY, Double.MIN_VALUE}
      )
  );
  //CHECKSTYLE.ON: Regexp

  public static final Dataset<HyperLogLogCollector> TEST_COMPLEX_HLL = new Dataset<>(
      HyperUniquesAggregatorFactory.TYPE,
      Arrays.asList(
          null,
          ByteRowKeyComparatorTest.makeHllCollector(1),
          ByteRowKeyComparatorTest.makeHllCollector(10),
          ByteRowKeyComparatorTest.makeHllCollector(50)
      )
  );

  // Sortedness of structured data depends on the hash value computed for the objects inside.
  public static final Dataset<StructuredData> TEST_COMPLEX_NESTED = new Dataset<>(
      ColumnType.NESTED_DATA,
      Stream.of(
          null,
          StructuredData.create("foo"),
          StructuredData.create("bar"),
          StructuredData.create(ImmutableMap.of("a", 100, "b", 200)),
          StructuredData.create(ImmutableMap.of("a", 100, "b", ImmutableList.of("x", "y"))),
          StructuredData.create(ImmutableMap.of("a", 100, "b", ImmutableMap.of("x", "y"))),
          StructuredData.wrap(100.1D),
          StructuredData.wrap(ImmutableList.of("p", "q", "r")),
          StructuredData.wrap(100),
          StructuredData.wrap(ImmutableList.of("p", "q", "r")),
          StructuredData.wrap(1000)
      ).sorted(Comparators.naturalNullsFirst()).collect(Collectors.toList())
  );

  /**
   * Wrapper around all the various TEST_* lists.
   */
  public static final List<Dataset<?>> DATASETS =
      ImmutableList.<Dataset<?>>builder()
                   .add(TEST_FLOATS)
                   .add(TEST_DOUBLES)
                   .add(TEST_LONGS)
                   .add(TEST_STRINGS_SINGLE_VALUE)
                   .add(TEST_STRINGS_MULTI_VALUE)
                   .add(TEST_ARRAYS_STRING)
                   .add(TEST_ARRAYS_LONG)
                   .add(TEST_ARRAYS_FLOAT)
                   .add(TEST_ARRAYS_DOUBLE)
                   .add(TEST_COMPLEX_HLL)
                   .add(TEST_COMPLEX_NESTED)
                   .build();

  public static class Dataset<T>
  {
    private final ColumnType type;
    private final List<T> sortedData;

    public Dataset(final ColumnType type, final List<T> sortedData)
    {
      this.type = type;
      this.sortedData = sortedData;
    }

    public ColumnType getType()
    {
      return type;
    }

    public List<T> getData(final KeyOrder sortedness)
    {
      switch (sortedness) {
        case ASCENDING:
          return Collections.unmodifiableList(sortedData);
        case DESCENDING:
          return Collections.unmodifiableList(Lists.reverse(sortedData));
        case NONE:
          // Shuffle ("unsort") the list, using the same seed every time for consistency.
          final List<T> newList = new ArrayList<>(sortedData);
          Collections.shuffle(newList, new Random(0));
          return newList;
        default:
          throw new ISE("No such sortedness [%s]", sortedness);
      }
    }
  }
}
