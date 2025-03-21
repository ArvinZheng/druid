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

package org.apache.druid.segment.join.table;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.ConstantDimensionSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.JoinMatcher;
import org.apache.druid.segment.join.Joinable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

public class IndexedTableJoinableTest
{
  private static final String PREFIX = "j.";
  private static final String KEY_COLUMN = "str";
  private static final String VALUE_COLUMN = "long";
  private static final String ALL_SAME_COLUMN = "allsame";
  private static final String UNKNOWN_COLUMN = "unknown";
  private static final String SEARCH_KEY_NULL_VALUE = "baz";
  private static final String SEARCH_KEY_VALUE = "foo";
  private static final String SEARCH_VALUE_VALUE = "1";
  private static final String SEARCH_VALUE_UNKNOWN = "10";
  private static final long MAX_CORRELATION_SET_SIZE = 10_000L;

  private final ColumnSelectorFactory dummyColumnSelectorFactory = new ColumnSelectorFactory()
  {
    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      return new ConstantDimensionSelector("dummy");
    }

    @Override
    public ColumnValueSelector<?> makeColumnValueSelector(String columnName)
    {
      return null;
    }

    @Override
    public ColumnCapabilities getColumnCapabilities(String columnName)
    {
      return null;
    }
  };

  private final InlineDataSource inlineDataSource = InlineDataSource.fromIterable(
      ImmutableList.of(
          new Object[]{"foo", 1L, 1L},
          new Object[]{"bar", 2L, 1L},
          new Object[]{"baz", null, 1L},
          new Object[]{null, 3L, 1L}
      ),
      RowSignature.builder()
                  .add(KEY_COLUMN, ColumnType.STRING)
                  .add(VALUE_COLUMN, ColumnType.LONG)
                  .add(ALL_SAME_COLUMN, ColumnType.LONG)
                  .build()
  );

  private final RowBasedIndexedTable<Object[]> indexedTable = new RowBasedIndexedTable<>(
      inlineDataSource.getRowsAsList(),
      inlineDataSource.rowAdapter(),
      inlineDataSource.getRowSignature(),
      ImmutableSet.of("str"),
      DateTimes.nowUtc().toString()
  );

  private IndexedTableJoinable target;

  @Before
  public void setUp()
  {
    target = new IndexedTableJoinable(indexedTable);
  }

  @Test
  public void getAvailableColumns()
  {
    Assert.assertEquals(ImmutableList.of(KEY_COLUMN, VALUE_COLUMN, ALL_SAME_COLUMN), target.getAvailableColumns());
  }

  @Test
  public void getCardinalityForStringColumn()
  {
    Assert.assertEquals(indexedTable.numRows() + 1, target.getCardinality("str"));
  }

  @Test
  public void getCardinalityForLongColumn()
  {
    Assert.assertEquals(indexedTable.numRows() + 1, target.getCardinality("long"));
  }

  @Test
  public void getCardinalityForNonexistentColumn()
  {
    Assert.assertEquals(1, target.getCardinality("nonexistent"));
  }

  @Test
  public void getColumnCapabilitiesForStringColumn()
  {
    final ColumnCapabilities capabilities = target.getColumnCapabilities("str");
    Assert.assertEquals(ValueType.STRING, capabilities.getType());
    Assert.assertTrue(capabilities.isDictionaryEncoded().isTrue());
    Assert.assertFalse(capabilities.hasBitmapIndexes());
    Assert.assertFalse(capabilities.hasMultipleValues().isMaybeTrue());
    Assert.assertFalse(capabilities.hasSpatialIndexes());
  }

  @Test
  public void getColumnCapabilitiesForLongColumn()
  {
    final ColumnCapabilities capabilities = target.getColumnCapabilities("long");
    Assert.assertEquals(ValueType.LONG, capabilities.getType());
    Assert.assertFalse(capabilities.isDictionaryEncoded().isTrue());
    Assert.assertFalse(capabilities.hasBitmapIndexes());
    Assert.assertFalse(capabilities.hasMultipleValues().isMaybeTrue());
    Assert.assertFalse(capabilities.hasSpatialIndexes());
  }

  @Test
  public void getColumnCapabilitiesForNonexistentColumnShouldReturnNull()
  {
    final ColumnCapabilities capabilities = target.getColumnCapabilities("nonexistent");
    Assert.assertNull(capabilities);
  }

  @Test
  public void makeJoinMatcherWithDimensionSelectorOnString()
  {
    final JoinConditionAnalysis condition = JoinConditionAnalysis.forExpression(
        "x == \"j.str\"",
        PREFIX,
        ExprMacroTable.nil()
    );
    final JoinMatcher joinMatcher = target.makeJoinMatcher(
        dummyColumnSelectorFactory,
        condition,
        false,
        Closer.create()
    );

    final DimensionSelector selector = joinMatcher.getColumnSelectorFactory()
                                                  .makeDimensionSelector(DefaultDimensionSpec.of("str"));

    // getValueCardinality
    Assert.assertEquals(5, selector.getValueCardinality());

    // nameLookupPossibleInAdvance
    Assert.assertTrue(selector.nameLookupPossibleInAdvance());

    // lookupName
    Assert.assertEquals("foo", selector.lookupName(0));
    Assert.assertEquals("bar", selector.lookupName(1));
    Assert.assertEquals("baz", selector.lookupName(2));
    Assert.assertNull(selector.lookupName(3));
    Assert.assertNull(selector.lookupName(4));

    // lookupId
    Assert.assertNull(selector.idLookup());
  }

  @Test
  public void getCorrelatedColummnValuesMissingSearchColumnShouldReturnEmpty()
  {
    Optional<InDimFilter.ValuesSet> correlatedValues =
        target.getCorrelatedColumnValues(
            UNKNOWN_COLUMN,
            "foo",
            VALUE_COLUMN,
            MAX_CORRELATION_SET_SIZE,
            false
        );

    Assert.assertEquals(Optional.empty(), correlatedValues);
  }

  @Test
  public void getCorrelatedColummnValuesMissingRetrievalColumnShouldReturnEmpty()
  {
    Optional<InDimFilter.ValuesSet> correlatedValues =
        target.getCorrelatedColumnValues(
            KEY_COLUMN,
            "foo",
            UNKNOWN_COLUMN,
            MAX_CORRELATION_SET_SIZE,
            false
        );

    Assert.assertEquals(Optional.empty(), correlatedValues);
  }

  @Test
  public void getCorrelatedColumnValuesForSearchKeyAndRetrieveKeyColumnShouldReturnSearchValue()
  {
    Optional<InDimFilter.ValuesSet> correlatedValues = target.getCorrelatedColumnValues(
        KEY_COLUMN,
        SEARCH_KEY_VALUE,
        KEY_COLUMN,
        MAX_CORRELATION_SET_SIZE,
        false
    );
    Assert.assertEquals(Optional.of(ImmutableSet.of(SEARCH_KEY_VALUE)), correlatedValues);
  }

  @Test
  public void getCorrelatedColumnValuesForSearchKeyAndRetrieveKeyColumnAboveLimitShouldReturnEmpty()
  {
    Optional<InDimFilter.ValuesSet> correlatedValues = target.getCorrelatedColumnValues(
        KEY_COLUMN,
        SEARCH_KEY_VALUE,
        KEY_COLUMN,
        0,
        false
    );
    Assert.assertEquals(Optional.empty(), correlatedValues);
  }

  @Test
  public void getCorrelatedColumnValuesForSearchKeyAndRetrieveValueColumnShouldReturnExtractedValue()
  {
    Optional<InDimFilter.ValuesSet> correlatedValues = target.getCorrelatedColumnValues(
        KEY_COLUMN,
        SEARCH_KEY_VALUE,
        VALUE_COLUMN,
        MAX_CORRELATION_SET_SIZE,
        false
    );
    Assert.assertEquals(Optional.of(ImmutableSet.of(SEARCH_VALUE_VALUE)), correlatedValues);
  }

  @Test
  public void getCorrelatedColumnValuesForSearchKeyMissingAndRetrieveValueColumnShouldReturnExtractedValue()
  {
    Optional<InDimFilter.ValuesSet> correlatedValues = target.getCorrelatedColumnValues(
        KEY_COLUMN,
        SEARCH_KEY_NULL_VALUE,
        VALUE_COLUMN,
        MAX_CORRELATION_SET_SIZE,
        false
    );
    Assert.assertEquals(Optional.of(Collections.singleton(null)), correlatedValues);
  }

  @Test
  public void getCorrelatedColumnValuesForSearchValueAndRetrieveValueColumnAndNonKeyColumnSearchDisabledShouldReturnEmpty()
  {
    Optional<InDimFilter.ValuesSet> correlatedValues = target.getCorrelatedColumnValues(
        VALUE_COLUMN,
        SEARCH_VALUE_VALUE,
        VALUE_COLUMN,
        MAX_CORRELATION_SET_SIZE,
        false
    );
    Assert.assertEquals(Optional.empty(), correlatedValues);
    correlatedValues = target.getCorrelatedColumnValues(
        VALUE_COLUMN,
        SEARCH_VALUE_VALUE,
        KEY_COLUMN,
        10,
        false
    );
    Assert.assertEquals(Optional.empty(), correlatedValues);
  }

  @Test
  public void getCorrelatedColumnValuesForSearchValueAndRetrieveValueColumnShouldReturnSearchValue()
  {
    Optional<InDimFilter.ValuesSet> correlatedValues = target.getCorrelatedColumnValues(
        VALUE_COLUMN,
        SEARCH_VALUE_VALUE,
        VALUE_COLUMN,
        MAX_CORRELATION_SET_SIZE,
        true
    );
    Assert.assertEquals(Optional.of(ImmutableSet.of(SEARCH_VALUE_VALUE)), correlatedValues);
  }

  @Test
  public void getCorrelatedColumnValuesForSearchValueAndRetrieveKeyColumnShouldReturnUnAppliedValue()
  {
    Optional<InDimFilter.ValuesSet> correlatedValues = target.getCorrelatedColumnValues(
        VALUE_COLUMN,
        SEARCH_VALUE_VALUE,
        KEY_COLUMN,
        10,
        true
    );
    Assert.assertEquals(Optional.of(ImmutableSet.of(SEARCH_KEY_VALUE)), correlatedValues);
  }

  @Test
  public void getCorrelatedColumnValuesForSearchValueAndRetrieveKeyColumnWithMaxLimitSetShouldHonorMaxLimit()
  {
    Optional<InDimFilter.ValuesSet> correlatedValues = target.getCorrelatedColumnValues(
        VALUE_COLUMN,
        SEARCH_VALUE_VALUE,
        KEY_COLUMN,
        0,
        true
    );
    Assert.assertEquals(Optional.empty(), correlatedValues);
  }

  @Test
  public void getCorrelatedColumnValuesForSearchUnknownValueAndRetrieveKeyColumnShouldReturnNoCorrelatedValues()
  {
    Optional<InDimFilter.ValuesSet> correlatedValues = target.getCorrelatedColumnValues(
        VALUE_COLUMN,
        SEARCH_VALUE_UNKNOWN,
        KEY_COLUMN,
        10,
        true
    );
    Assert.assertEquals(Optional.of(ImmutableSet.of()), correlatedValues);
  }

  @Test
  public void getMatchableColumnValuesIfAllUniqueForValueColumnShouldReturnValues()
  {
    final Joinable.ColumnValuesWithUniqueFlag values =
        target.getMatchableColumnValues(VALUE_COLUMN, false, Integer.MAX_VALUE);

    Assert.assertEquals(ImmutableSet.of("1", "2", "3"), values.getColumnValues());
  }

  @Test
  public void getMatchableColumnValuesIfAllUniqueForNonexistentColumnShouldReturnEmpty()
  {
    final Joinable.ColumnValuesWithUniqueFlag values =
        target.getMatchableColumnValues("nonexistent", false, Integer.MAX_VALUE);

    Assert.assertEquals(ImmutableSet.of(), values.getColumnValues());
  }

  @Test
  public void getMatchableColumnValuesIfAllUniqueForKeyColumnShouldReturnValues()
  {
    final Joinable.ColumnValuesWithUniqueFlag values =
        target.getMatchableColumnValues(KEY_COLUMN, false, Integer.MAX_VALUE);

    Assert.assertEquals(
        ImmutableSet.of("foo", "bar", "baz"),
        values.getColumnValues()
    );

    Assert.assertTrue(values.isAllUnique());
  }

  @Test
  public void getMatchableColumnValuesWithIncludeNullIfAllUniqueForKeyColumnShouldReturnValues()
  {
    final Joinable.ColumnValuesWithUniqueFlag values =
        target.getMatchableColumnValues(KEY_COLUMN, true, Integer.MAX_VALUE);

    Assert.assertEquals(
        InDimFilter.ValuesSet.copyOf(Arrays.asList(null, "foo", "bar", "baz")),
        values.getColumnValues()
    );

    Assert.assertTrue(values.isAllUnique());
  }

  @Test
  public void getMatchableColumnValuesIfAllUniqueForAllSameColumnShouldReturnEmpty()
  {
    final Joinable.ColumnValuesWithUniqueFlag values =
        target.getMatchableColumnValues(ALL_SAME_COLUMN, false, Integer.MAX_VALUE);

    Assert.assertEquals(
        ImmutableSet.of("1"),
        values.getColumnValues()
    );
    Assert.assertFalse(values.isAllUnique());
  }

  @Test
  public void getMatchableColumnValuesIfAllUniqueForKeyColumnWithLowMaxValuesShouldReturnEmpty()
  {
    final Joinable.ColumnValuesWithUniqueFlag values =
        target.getMatchableColumnValues(KEY_COLUMN, false, 1);

    Assert.assertEquals(ImmutableSet.of(), values.getColumnValues());
  }
}
