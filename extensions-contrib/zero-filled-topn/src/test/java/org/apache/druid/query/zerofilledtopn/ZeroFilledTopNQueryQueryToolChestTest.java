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

package org.apache.druid.query.zerofilledtopn;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.druid.collections.SerializablePair;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.CacheStrategy;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.Result;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.SerializablePairLongString;
import org.apache.druid.query.aggregation.cardinality.CardinalityAggregator;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.aggregation.last.DoubleLastAggregatorFactory;
import org.apache.druid.query.aggregation.last.FloatLastAggregatorFactory;
import org.apache.druid.query.aggregation.last.LongLastAggregatorFactory;
import org.apache.druid.query.aggregation.last.StringLastAggregatorFactory;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.ConstantPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.aggregation.post.FinalizingFieldAccessPostAggregator;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.topn.LegacyTopNMetricSpec;
import org.apache.druid.query.topn.NumericTopNMetricSpec;
import org.apache.druid.query.topn.TopNResultValue;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.timeline.SegmentId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

public class ZeroFilledTopNQueryQueryToolChestTest
{

  private static final SegmentId SEGMENT_ID = SegmentId.dummy("testSegment");

  @Before
  public void setup()
  {
    NullHandling.initializeForTests();
  }

  @Test
  public void testCacheStrategy() throws Exception
  {
    doTestCacheStrategy(ValueType.STRING, "val1");
    doTestCacheStrategy(ValueType.FLOAT, 2.1f);
    doTestCacheStrategy(ValueType.DOUBLE, 2.1d);
    doTestCacheStrategy(ValueType.LONG, 2L);
  }

  @Test
  public void testCacheStrategyOrderByPostAggs() throws Exception
  {
    doTestCacheStrategyOrderByPost(ValueType.STRING, "val1");
    doTestCacheStrategyOrderByPost(ValueType.FLOAT, 2.1f);
    doTestCacheStrategyOrderByPost(ValueType.DOUBLE, 2.1d);
    doTestCacheStrategyOrderByPost(ValueType.LONG, 2L);
  }

  @Test
  public void testComputeCacheKeyWithDifferentPostAgg()
  {
    final ZeroFilledTopNQuery query1 = new ZeroFilledTopNQuery(
        new TableDataSource("dummy"),
        VirtualColumns.EMPTY,
        new DefaultDimensionSpec("test", "test"),
        new NumericTopNMetricSpec("post"),
        3,
        new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2015-01-01/2015-01-02"))),
        null,
        Granularities.ALL,
        ImmutableList.of(new CountAggregatorFactory("metric1")),
        ImmutableList.of(new ConstantPostAggregator("post", 10)),
        ImmutableMap.of(ZeroFilledTopNQuery.DIM_VALUES, Lists.newArrayList("1", "2", "3"))
    );

    final ZeroFilledTopNQuery query2 = new ZeroFilledTopNQuery(
        new TableDataSource("dummy"),
        VirtualColumns.EMPTY,
        new DefaultDimensionSpec("test", "test"),
        new NumericTopNMetricSpec("post"),
        3,
        new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2015-01-01/2015-01-02"))),
        null,
        Granularities.ALL,
        ImmutableList.of(new CountAggregatorFactory("metric1")),
        ImmutableList.of(
            new ArithmeticPostAggregator(
                "post",
                "+",
                ImmutableList.of(
                    new FieldAccessPostAggregator(
                        null,
                        "metric1"
                    ),
                    new FieldAccessPostAggregator(
                        null,
                        "metric1"
                    )
                )
            )
        ),
        ImmutableMap.of(ZeroFilledTopNQuery.DIM_VALUES, Lists.newArrayList("1", "2", "3"))
    );

    final CacheStrategy<Result<TopNResultValue>, Object, ZeroFilledTopNQuery> strategy1 = new ZeroFilledTopNQueryQueryToolChest(
        null,
        null
    ).getCacheStrategy(query1);

    final CacheStrategy<Result<TopNResultValue>, Object, ZeroFilledTopNQuery> strategy2 = new ZeroFilledTopNQueryQueryToolChest(
        null,
        null
    ).getCacheStrategy(query2);

    Assert.assertFalse(Arrays.equals(strategy1.computeCacheKey(query1), strategy2.computeCacheKey(query2)));
    Assert.assertFalse(Arrays.equals(
        strategy1.computeResultLevelCacheKey(query1),
        strategy2.computeResultLevelCacheKey(query2)
    ));
  }

  @Test
  public void testComputeResultLevelCacheKeyWithDifferentPostAgg()
  {
    final ZeroFilledTopNQuery query1 = new ZeroFilledTopNQuery(
        new TableDataSource("dummy"),
        VirtualColumns.EMPTY,
        new DefaultDimensionSpec("test", "test"),
        new LegacyTopNMetricSpec("metric1"),
        3,
        new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2015-01-01T18:00:00/2015-01-02T18:00:00"))),
        null,
        Granularities.ALL,
        ImmutableList.of(
            new LongSumAggregatorFactory("metric1", "metric1"),
            new LongSumAggregatorFactory("metric2", "metric2")
        ),
        ImmutableList.of(
            new ArithmeticPostAggregator(
                "post1",
                "/",
                ImmutableList.of(
                    new FieldAccessPostAggregator(
                        "metric1",
                        "metric1"
                    ),
                    new FieldAccessPostAggregator(
                        "metric2",
                        "metric2"
                    )
                )
            )
        ),
        ImmutableMap.of(ZeroFilledTopNQuery.DIM_VALUES, Lists.newArrayList("1", "2", "3"))
    );

    final ZeroFilledTopNQuery query2 = new ZeroFilledTopNQuery(
        new TableDataSource("dummy"),
        VirtualColumns.EMPTY,
        new DefaultDimensionSpec("test", "test"),
        new LegacyTopNMetricSpec("metric1"),
        3,
        new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2015-01-01T18:00:00/2015-01-02T18:00:00"))),
        null,
        Granularities.ALL,
        ImmutableList.of(
            new LongSumAggregatorFactory("metric1", "metric1"),
            new LongSumAggregatorFactory("metric2", "metric2")
        ),
        ImmutableList.of(
            new ArithmeticPostAggregator(
                "post2",
                "+",
                ImmutableList.of(
                    new FieldAccessPostAggregator(
                        "metric1",
                        "metric1"
                    ),
                    new FieldAccessPostAggregator(
                        "metric2",
                        "metric2"
                    )
                )
            )
        ),
        ImmutableMap.of(ZeroFilledTopNQuery.DIM_VALUES, Lists.newArrayList("1", "2", "3"))
    );

    final CacheStrategy<Result<TopNResultValue>, Object, ZeroFilledTopNQuery> strategy1 = new ZeroFilledTopNQueryQueryToolChest(
        null,
        null
    ).getCacheStrategy(query1);

    final CacheStrategy<Result<TopNResultValue>, Object, ZeroFilledTopNQuery> strategy2 = new ZeroFilledTopNQueryQueryToolChest(
        null,
        null
    ).getCacheStrategy(query2);

    //segment level cache key excludes postaggregates in topn
    Assert.assertTrue(Arrays.equals(strategy1.computeCacheKey(query1), strategy2.computeCacheKey(query2)));
    Assert.assertFalse(Arrays.equals(strategy1.computeCacheKey(query1), strategy1.computeResultLevelCacheKey(query1)));
    Assert.assertFalse(Arrays.equals(
        strategy1.computeResultLevelCacheKey(query1),
        strategy2.computeResultLevelCacheKey(query2)
    ));
  }

  private AggregatorFactory getComplexAggregatorFactoryForValueType(final ValueType valueType)
  {
    switch (valueType) {
      case LONG:
        return new LongLastAggregatorFactory("complexMetric", "test");
      case DOUBLE:
        return new DoubleLastAggregatorFactory("complexMetric", "test");
      case FLOAT:
        return new FloatLastAggregatorFactory("complexMetric", "test");
      case STRING:
        return new StringLastAggregatorFactory("complexMetric", "test", null);
      default:
        throw new IllegalArgumentException("bad valueType: " + valueType);
    }
  }

  private SerializablePair getIntermediateComplexValue(final ValueType valueType, final Object dimValue)
  {
    switch (valueType) {
      case LONG:
      case DOUBLE:
      case FLOAT:
        return new SerializablePair<>(123L, dimValue);
      case STRING:
        return new SerializablePairLongString(123L, (String) dimValue);
      default:
        throw new IllegalArgumentException("bad valueType: " + valueType);
    }
  }

  private HyperLogLogCollector getIntermediateHllCollector(final ValueType valueType, final Object dimValue)
  {
    HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();
    switch (valueType) {
      case LONG:
        collector.add(CardinalityAggregator.HASH_FUNCTION.hashLong((Long) dimValue).asBytes());
        break;
      case DOUBLE:
        collector.add(CardinalityAggregator.HASH_FUNCTION.hashLong(Double.doubleToLongBits((Double) dimValue))
                                                         .asBytes());
        break;
      case FLOAT:
        collector.add(CardinalityAggregator.HASH_FUNCTION.hashInt(Float.floatToIntBits((Float) dimValue)).asBytes());
        break;
      case STRING:
        collector.add(CardinalityAggregator.HASH_FUNCTION.hashUnencodedChars((String) dimValue).asBytes());
        break;
      default:
        throw new IllegalArgumentException("bad valueType: " + valueType);
    }
    return collector;
  }

  private void doTestCacheStrategy(final ValueType valueType, final Object dimValue) throws IOException
  {
    CacheStrategy<Result<TopNResultValue>, Object, ZeroFilledTopNQuery> strategy =
        new ZeroFilledTopNQueryQueryToolChest(null, null).getCacheStrategy(
            new ZeroFilledTopNQuery(
                new TableDataSource("dummy"),
                VirtualColumns.EMPTY,
                new DefaultDimensionSpec("test", "test", valueType),
                new NumericTopNMetricSpec("metric1"),
                3,
                new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2015-01-01/2015-01-02"))),
                null,
                Granularities.ALL,
                ImmutableList.of(
                    new CountAggregatorFactory("metric1"),
                    getComplexAggregatorFactoryForValueType(valueType)
                ),
                ImmutableList.of(new ConstantPostAggregator("post", 10)),
                ImmutableMap.of(ZeroFilledTopNQuery.DIM_VALUES, Lists.newArrayList("1", "2", "3"))
            )
        );

    final Result<TopNResultValue> result1 = new Result<>(
        // test timestamps that result in integer size millis
        DateTimes.utc(123L),
        new TopNResultValue(
            Collections.singletonList(
                ImmutableMap.of(
                    "test", dimValue,
                    "metric1", 2,
                    "complexMetric", getIntermediateComplexValue(valueType, dimValue)
                )
            )
        )
    );

    Object preparedValue = strategy.prepareForSegmentLevelCache().apply(
        result1
    );

    ObjectMapper objectMapper = TestHelper.makeJsonMapper();
    Object fromCacheValue = objectMapper.readValue(
        objectMapper.writeValueAsBytes(preparedValue),
        strategy.getCacheObjectClazz()
    );

    Result<TopNResultValue> fromCacheResult = strategy.pullFromSegmentLevelCache().apply(fromCacheValue);

    Assert.assertEquals(result1, fromCacheResult);

    final Result<TopNResultValue> result2 = new Result<>(
        // test timestamps that result in integer size millis
        DateTimes.utc(123L),
        new TopNResultValue(
            Collections.singletonList(
                ImmutableMap.of(
                    "test", dimValue,
                    "metric1", 2,
                    "complexMetric", dimValue,
                    "post", 10
                )
            )
        )
    );

    // Please see the comments on aggregator serde and type handling in CacheStrategy.fetchAggregatorsFromCache()
    final Result<TopNResultValue> typeAdjustedResult2;
    if (valueType == ValueType.FLOAT) {
      typeAdjustedResult2 = new Result<>(
          DateTimes.utc(123L),
          new TopNResultValue(
              Collections.singletonList(
                  ImmutableMap.of(
                      "test", dimValue,
                      "metric1", 2,
                      "complexMetric", 2.1d,
                      "post", 10
                  )
              )
          )
      );
    } else if (valueType == ValueType.LONG) {
      typeAdjustedResult2 = new Result<>(
          DateTimes.utc(123L),
          new TopNResultValue(
              Collections.singletonList(
                  ImmutableMap.of(
                      "test", dimValue,
                      "metric1", 2,
                      "complexMetric", 2,
                      "post", 10
                  )
              )
          )
      );
    } else {
      typeAdjustedResult2 = result2;
    }


    Object preparedResultCacheValue = strategy.prepareForCache(true).apply(
        result2
    );

    Object fromResultCacheValue = objectMapper.readValue(
        objectMapper.writeValueAsBytes(preparedResultCacheValue),
        strategy.getCacheObjectClazz()
    );

    Result<TopNResultValue> fromResultCacheResult = strategy.pullFromCache(true).apply(fromResultCacheValue);
    Assert.assertEquals(typeAdjustedResult2, fromResultCacheResult);
  }

  private void doTestCacheStrategyOrderByPost(final ValueType valueType, final Object dimValue) throws IOException
  {
    CacheStrategy<Result<TopNResultValue>, Object, ZeroFilledTopNQuery> strategy =
        new ZeroFilledTopNQueryQueryToolChest(null, null).getCacheStrategy(
            new ZeroFilledTopNQuery(
                new TableDataSource("dummy"),
                VirtualColumns.EMPTY,
                new DefaultDimensionSpec("test", "test", valueType),
                new NumericTopNMetricSpec("post"),
                3,
                new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2015-01-01/2015-01-02"))),
                null,
                Granularities.ALL,
                ImmutableList.of(
                    new HyperUniquesAggregatorFactory("metric1", "test", false, false),
                    new CountAggregatorFactory("metric2")
                ),
                ImmutableList.of(
                    new ArithmeticPostAggregator(
                        "post",
                        "+",
                        ImmutableList.of(
                            new FinalizingFieldAccessPostAggregator(
                                "metric1",
                                "metric1"
                            ),
                            new FieldAccessPostAggregator(
                                "metric2",
                                "metric2"
                            )
                        )
                    )
                ),
                ImmutableMap.of(ZeroFilledTopNQuery.DIM_VALUES, Lists.newArrayList("1", "2", "3"))
            )
        );

    HyperLogLogCollector collector = getIntermediateHllCollector(valueType, dimValue);

    final Result<TopNResultValue> result1 = new Result<>(
        // test timestamps that result in integer size millis
        DateTimes.utc(123L),
        new TopNResultValue(
            Collections.singletonList(
                ImmutableMap.of(
                    "test", dimValue,
                    "metric1", collector,
                    "metric2", 2,
                    "post", collector.estimateCardinality() + 2
                )
            )
        )
    );

    Object preparedValue = strategy.prepareForSegmentLevelCache().apply(
        result1
    );

    ObjectMapper objectMapper = TestHelper.makeJsonMapper();
    Object fromCacheValue = objectMapper.readValue(
        objectMapper.writeValueAsBytes(preparedValue),
        strategy.getCacheObjectClazz()
    );

    Result<TopNResultValue> fromCacheResult = strategy.pullFromSegmentLevelCache().apply(fromCacheValue);

    Assert.assertEquals(result1, fromCacheResult);

    final Result<TopNResultValue> resultLevelCacheResult = new Result<>(
        // test timestamps that result in integer size millis
        DateTimes.utc(123L),
        new TopNResultValue(
            Collections.singletonList(
                ImmutableMap.of(
                    "test", dimValue,
                    "metric1", collector.estimateCardinality(),
                    "metric2", 2,
                    "post", collector.estimateCardinality() + 2
                )
            )
        )
    );

    Object preparedResultCacheValue = strategy.prepareForCache(true).apply(
        resultLevelCacheResult
    );

    Object fromResultCacheValue = objectMapper.readValue(
        objectMapper.writeValueAsBytes(preparedResultCacheValue),
        strategy.getCacheObjectClazz()
    );

    Result<TopNResultValue> fromResultCacheResult = strategy.pullFromCache(true).apply(fromResultCacheValue);
    Assert.assertEquals(resultLevelCacheResult, fromResultCacheResult);
  }

  static class MockQueryRunner implements QueryRunner<Result<TopNResultValue>>
  {
    private final QueryRunner<Result<TopNResultValue>> runner;
    ZeroFilledTopNQuery query = null;

    MockQueryRunner(QueryRunner<Result<TopNResultValue>> runner)
    {
      this.runner = runner;
    }

    @Override
    public Sequence<Result<TopNResultValue>> run(
        QueryPlus<Result<TopNResultValue>> queryPlus,
        ResponseContext responseContext
    )
    {
      this.query = (ZeroFilledTopNQuery) queryPlus.getQuery();
      return runner.run(queryPlus, responseContext);
    }
  }

}
