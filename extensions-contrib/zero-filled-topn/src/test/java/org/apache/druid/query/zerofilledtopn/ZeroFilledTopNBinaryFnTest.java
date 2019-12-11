package org.apache.druid.query.zerofilledtopn;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.ConstantPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.topn.InvertedTopNMetricSpec;
import org.apache.druid.query.topn.NumericTopNMetricSpec;
import org.apache.druid.query.topn.TopNResultValue;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ZeroFilledTopNBinaryFnTest {

    final CountAggregatorFactory rowsCount = new CountAggregatorFactory("rows");
    final LongSumAggregatorFactory indexLongSum = new LongSumAggregatorFactory("index", "index");
    final ConstantPostAggregator constant = new ConstantPostAggregator("const", 1L);
    final FieldAccessPostAggregator rowsPostAgg = new FieldAccessPostAggregator("rows", "rows");
    final FieldAccessPostAggregator indexPostAgg = new FieldAccessPostAggregator("index", "index");
    final ArithmeticPostAggregator addrowsindexconstant = new ArithmeticPostAggregator(
            "addrowsindexconstant",
            "+",
            Lists.newArrayList(constant, rowsPostAgg, indexPostAgg)
    );

    final List<AggregatorFactory> aggregatorFactories = Arrays.asList(
            rowsCount,
            indexLongSum
    );

    final List<PostAggregator> postAggregators = Collections.singletonList(
            addrowsindexconstant
    );

    final Set<String> dimValues = Sets.newHashSet("1", "2", "3");

    private final DateTime currTime = DateTimes.nowUtc();

    private void assertTopNMergeResult(Object o1, Object o2) {
        Iterator i1 = ((Iterable) o1).iterator();
        Iterator i2 = ((Iterable) o2).iterator();
        while (i1.hasNext() && i2.hasNext()) {
            Assert.assertEquals(i1.next(), i2.next());
        }
        Assert.assertTrue(!i1.hasNext() && !i2.hasNext());
    }

    @Test
    public void testMerge_Expect_MergedProperlyByDimension() {
        Result<TopNResultValue> result1 = new Result<TopNResultValue>(
                currTime,
                new TopNResultValue(
                        ImmutableList.<Map<String, Object>>of(
                                ImmutableMap.of(
                                        "rows", 1L,
                                        "index", 2L,
                                        "testdim", "1"
                                ),
                                ImmutableMap.of(
                                        "rows", 2L,
                                        "index", 4L,
                                        "testdim", "2"
                                ),
                                ImmutableMap.of(
                                        "rows", 0L,
                                        "index", 2L,
                                        "testdim", "3"
                                )
                        )
                )
        );
        Result<TopNResultValue> result2 = new Result<TopNResultValue>(
                currTime,
                new TopNResultValue(
                        ImmutableList.<Map<String, Object>>of(
                                ImmutableMap.of(
                                        "testdim", "1",
                                        "rows", 2L,
                                        "index", 3L

                                ),
                                ImmutableMap.of(
                                        "testdim", "2",
                                        "rows", 2L,
                                        "index", 0L

                                ),
                                ImmutableMap.of(
                                        "testdim", "3",
                                        "rows", 0L,
                                        "index", 1L
                                )
                        )
                )
        );

        Result<TopNResultValue> expected = new Result<TopNResultValue>(
                currTime,
                new TopNResultValue(
                        ImmutableList.<Map<String, Object>>of(
                                ImmutableMap.of(
                                        "testdim", "1",
                                        "rows", 3L,
                                        "index", 5L
                                ),
                                ImmutableMap.of(
                                        "testdim", "2",
                                        "rows", 4L,
                                        "index", 4L
                                ),
                                ImmutableMap.of(
                                        "testdim", "3",
                                        "rows", 0L,
                                        "index", 3L
                                )
                        )
                )
        );

        Result<TopNResultValue> actual = new ZeroFilledTopNBinaryFn(
                Granularities.ALL,
                new DefaultDimensionSpec("testdim", null),
                new NumericTopNMetricSpec("index"),
                3,
                aggregatorFactories,
                postAggregators,
                dimValues
        ).apply(
                result1,
                result2
        );
        Assert.assertEquals(expected.getTimestamp(), actual.getTimestamp());
        assertTopNMergeResult(expected.getValue(), actual.getValue());
    }

    @Test
    public void testMergeDay_Expect_MergedProperlyAtDayGranularity() {
        Result<TopNResultValue> result1 = new Result<TopNResultValue>(
                currTime,
                new TopNResultValue(
                        ImmutableList.<Map<String, Object>>of(
                                ImmutableMap.of(
                                        "rows", 1L,
                                        "index", 2L,
                                        "testdim", "1"
                                ),
                                ImmutableMap.of(
                                        "rows", 2L,
                                        "index", 4L,
                                        "testdim", "2"
                                ),
                                ImmutableMap.of(
                                        "rows", 0L,
                                        "index", 2L,
                                        "testdim", "3"
                                )
                        )
                )
        );
        Result<TopNResultValue> result2 = new Result<TopNResultValue>(
                currTime,
                new TopNResultValue(
                        ImmutableList.<Map<String, Object>>of(
                                ImmutableMap.of(
                                        "rows", 2L,
                                        "index", 3L,
                                        "testdim", "1"
                                ),
                                ImmutableMap.of(
                                        "rows", 2L,
                                        "index", 0L,
                                        "testdim", "2"
                                ),
                                ImmutableMap.of(
                                        "rows", 0L,
                                        "index", 1L,
                                        "testdim", "3"
                                )
                        )
                )
        );

        Result<TopNResultValue> expected = new Result<TopNResultValue>(
                Granularities.DAY.bucketStart(currTime),
                new TopNResultValue(
                        ImmutableList.<Map<String, Object>>of(
                                ImmutableMap.of(
                                        "testdim", "1",
                                        "rows", 3L,
                                        "index", 5L
                                ),
                                ImmutableMap.of(
                                        "testdim", "2",
                                        "rows", 4L,
                                        "index", 4L
                                ),
                                ImmutableMap.of(
                                        "testdim", "3",
                                        "rows", 0L,
                                        "index", 3L
                                )
                        )
                )
        );

        Result<TopNResultValue> actual = new ZeroFilledTopNBinaryFn(
                Granularities.DAY,
                new DefaultDimensionSpec("testdim", null),
                new NumericTopNMetricSpec("index"),
                3,
                aggregatorFactories,
                postAggregators,
                dimValues
        ).apply(
                result1,
                result2
        );
        Assert.assertEquals(expected.getTimestamp(), actual.getTimestamp());
        assertTopNMergeResult(expected.getValue(), actual.getValue());
    }

    @Test
    public void testMerge_WhenOneResultIsNull_Expect_NullResultIsIgnored() {
        Result<TopNResultValue> result1 = new Result<TopNResultValue>(
                currTime,
                new TopNResultValue(
                        ImmutableList.<Map<String, Object>>of(
                                ImmutableMap.of(
                                        "testdim", "2",
                                        "rows", 2L,
                                        "index", 4L
                                ),
                                ImmutableMap.of(
                                        "testdim", "1",
                                        "rows", 1L,
                                        "index", 2L
                                ),
                                ImmutableMap.of(
                                        "testdim", "3",
                                        "rows", 0L,
                                        "index", 2L
                                )
                        )
                )
        );
        Result<TopNResultValue> result2 = null;

        Result<TopNResultValue> expected = result1;

        Result<TopNResultValue> actual = new ZeroFilledTopNBinaryFn(
                Granularities.ALL,
                new DefaultDimensionSpec("testdim", null),
                new NumericTopNMetricSpec("index"),
                3,
                aggregatorFactories,
                postAggregators,
                dimValues
        ).apply(
                result1,
                result2
        );
        Assert.assertEquals(expected.getTimestamp(), actual.getTimestamp());
        assertTopNMergeResult(expected.getValue(), actual.getValue());
    }

    /**
     * TODO: the pre-zero fill break the compatibility with TopN query, we sort the segment based result
     */
    @Test
    public void testMergeByPostAgg_When_MergedMetricsAreEqual_Expect_SortedByDimValueDesc() {
        Result<TopNResultValue> result1 = new Result<TopNResultValue>(
                currTime,
                new TopNResultValue(
                        ImmutableList.<Map<String, Object>>of(
                                ImmutableMap.of(
                                        "rows", 1L,
                                        "index", 2L,
                                        "testdim", "1",
                                        "addrowsindexconstant", 3.0
                                ),
                                ImmutableMap.of(
                                        "rows", 2L,
                                        "index", 4L,
                                        "testdim", "2",
                                        "addrowsindexconstant", 7.0
                                ),
                                ImmutableMap.of(
                                        "rows", 0L,
                                        "index", 2L,
                                        "testdim", "3",
                                        "addrowsindexconstant", 3.0
                                )
                        )
                )
        );
        Result<TopNResultValue> result2 = new Result<TopNResultValue>(
                currTime,
                new TopNResultValue(
                        ImmutableList.<Map<String, Object>>of(
                                ImmutableMap.of(
                                        "rows", 2L,
                                        "index", 3L,
                                        "testdim", "1",
                                        "addrowsindexconstant", 6.0
                                ),
                                ImmutableMap.of(
                                        "rows", 2L,
                                        "index", 0L,
                                        "testdim", "2",
                                        "addrowsindexconstant", 3.0
                                ),
                                ImmutableMap.of(
                                        "rows", 4L,
                                        "index", 5L,
                                        "testdim", "other",
                                        "addrowsindexconstant", 10.0
                                )
                        )
                )
        );

        Result<TopNResultValue> expected = new Result<TopNResultValue>(
                currTime,
                new TopNResultValue(
                        ImmutableList.<Map<String, Object>>of(
                                ImmutableMap.of(
                                        "testdim", "other",
                                        "rows", 4L,
                                        "index", 5L,
                                        "addrowsindexconstant", 10.0
                                ),
                                ImmutableMap.of(
                                        "testdim", "1",
                                        "rows", 3L,
                                        "index", 5L,
                                        "addrowsindexconstant", 9.0
                                ),
                                ImmutableMap.of(
                                        "testdim", "2",
                                        "rows", 4L,
                                        "index", 4L,
                                        "addrowsindexconstant", 9.0
                                )
                        )
                )
        );

        Result<TopNResultValue> actual = new ZeroFilledTopNBinaryFn(
                Granularities.ALL,
                new DefaultDimensionSpec("testdim", null),
                new NumericTopNMetricSpec("addrowsindexconstant"),
                3,
                aggregatorFactories,
                postAggregators,
                dimValues
        ).apply(
                result1,
                result2
        );
        Assert.assertEquals(expected.getTimestamp(), actual.getTimestamp());
        assertTopNMergeResult(expected.getValue(), actual.getValue());
    }

    @Test
    public void testMergeShiftedTimestamp() {
        Result<TopNResultValue> result1 = new Result<TopNResultValue>(
                currTime,
                new TopNResultValue(
                        ImmutableList.<Map<String, Object>>of(
                                ImmutableMap.of(
                                        "rows", 1L,
                                        "index", 2L,
                                        "testdim", "1"
                                ),
                                ImmutableMap.of(
                                        "rows", 2L,
                                        "index", 4L,
                                        "testdim", "2"
                                ),
                                ImmutableMap.of(
                                        "rows", 0L,
                                        "index", 2L,
                                        "testdim", "3"
                                )
                        )
                )
        );
        Result<TopNResultValue> result2 = new Result<TopNResultValue>(
                currTime.plusHours(2),
                new TopNResultValue(
                        ImmutableList.<Map<String, Object>>of(
                                ImmutableMap.of(
                                        "rows", 2L,
                                        "index", 3L,
                                        "testdim", "1"
                                ),
                                ImmutableMap.of(
                                        "rows", 2L,
                                        "index", 0L,
                                        "testdim", "2"
                                ),
                                ImmutableMap.of(
                                        "rows", 0L,
                                        "index", 1L,
                                        "testdim", "3"
                                )
                        )
                )
        );

        Result<TopNResultValue> expected = new Result<TopNResultValue>(
                currTime,
                new TopNResultValue(
                        ImmutableList.<Map<String, Object>>of(
                                ImmutableMap.of(
                                        "testdim", "1",
                                        "rows", 3L,
                                        "index", 5L
                                ),
                                ImmutableMap.of(
                                        "testdim", "2",
                                        "rows", 4L,
                                        "index", 4L
                                ),
                                ImmutableMap.of(
                                        "testdim", "3",
                                        "rows", 0L,
                                        "index", 3L
                                )
                        )
                )
        );

        Result<TopNResultValue> actual = new ZeroFilledTopNBinaryFn(
                Granularities.ALL,
                new DefaultDimensionSpec("testdim", null),
                new NumericTopNMetricSpec("index"),
                3,
                aggregatorFactories,
                postAggregators,
                dimValues
        ).apply(
                result1,
                result2
        );
        Assert.assertEquals(expected.getTimestamp(), actual.getTimestamp());
        assertTopNMergeResult(expected.getValue(), actual.getValue());
    }

//    @Test(expected = IAE.class)
//    public void testDimensionTopNMetricSpec_Expect_IAE() {
//
//        Result<TopNResultValue> actual = new ZeroFilledTopNBinaryFn(
//                Granularities.ALL,
//                new DefaultDimensionSpec("testdim", "INVALID_DIM_NAME"),
//                new ZeroFilledNumericTopNMetricSpec(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC)),
//                2,
//                aggregatorFactories,
//                postAggregators,
//                dimValues
//        ).apply(
//                null,
//                null
//        );
//    }

    @Test
    public void testMerge_When_Inverted_Expect_AllZeroFilled() {
        Result<TopNResultValue> result1 = new Result<TopNResultValue>(
                currTime,
                new TopNResultValue(
                        ImmutableList.<Map<String, Object>>of(
                                ImmutableMap.of(
                                        "rows", 1L,
                                        "index", 2L,
                                        "testdim", "11"
                                ),
                                ImmutableMap.of(
                                        "rows", 2L,
                                        "index", 4L,
                                        "testdim", "22"
                                )
                        )
                )
        );
        Result<TopNResultValue> result2 = new Result<TopNResultValue>(
                currTime,
                new TopNResultValue(
                        ImmutableList.<Map<String, Object>>of(
                                ImmutableMap.of(
                                        "rows", 2L,
                                        "index", 3L,
                                        "testdim", "12"
                                ),
                                ImmutableMap.of(
                                        "rows", 2L,
                                        "index", 0L,
                                        "testdim", "22"
                                )
                        )
                )
        );

        Result<TopNResultValue> expected = new Result<TopNResultValue>(
                currTime,
                new TopNResultValue(
                        ImmutableList.<Map<String, Object>>of(
                                ImmutableMap.of(
                                        "testdim", "1",
                                        "rows", 0L,
                                        "index", 0L
                                ),
                                ImmutableMap.of(
                                        "testdim", "2",
                                        "rows", 0L,
                                        "index", 0L
                                ),
                                ImmutableMap.of(
                                        "testdim", "3",
                                        "rows", 0L,
                                        "index", 0L
                                )
                        )
                )
        );

        Result<TopNResultValue> actual = new ZeroFilledTopNBinaryFn(
                Granularities.ALL,
                new DefaultDimensionSpec("testdim", null),
                new InvertedTopNMetricSpec(new NumericTopNMetricSpec("index")),
                3,
                aggregatorFactories,
                postAggregators,
                dimValues
        ).apply(
                result1,
                result2
        );
        Assert.assertEquals(expected.getTimestamp(), actual.getTimestamp());
        assertTopNMergeResult(expected.getValue(), actual.getValue());
    }

    @Test
    public void testMerge_When_Inverted_Expect_ZeroFilledTwo() {
        Result<TopNResultValue> result1 = new Result<TopNResultValue>(
                currTime,
                new TopNResultValue(
                        ImmutableList.<Map<String, Object>>of(
                                ImmutableMap.of(
                                        "rows", 1L,
                                        "index", 2L,
                                        "testdim", "1"
                                ),
                                ImmutableMap.of(
                                        "rows", 2L,
                                        "index", 4L,
                                        "testdim", "22"
                                )
                        )
                )
        );
        Result<TopNResultValue> result2 = new Result<TopNResultValue>(
                currTime,
                new TopNResultValue(
                        ImmutableList.<Map<String, Object>>of(
                                ImmutableMap.of(
                                        "rows", 2L,
                                        "index", 3L,
                                        "testdim", "22"
                                ),
                                ImmutableMap.of(
                                        "rows", 2L,
                                        "index", 0L,
                                        "testdim", "1"
                                )
                        )
                )
        );

        Result<TopNResultValue> expected = new Result<TopNResultValue>(
                currTime,
                new TopNResultValue(
                        ImmutableList.<Map<String, Object>>of(
                                ImmutableMap.of(
                                        "testdim", "2",
                                        "rows", 0L,
                                        "index", 0L
                                ),
                                ImmutableMap.of(
                                        "testdim", "3",
                                        "rows", 0L,
                                        "index", 0L
                                ),
                                ImmutableMap.of(
                                        "testdim", "1",
                                        "rows", 3L,
                                        "index", 2L
                                )
                        )
                )
        );

        Result<TopNResultValue> actual = new ZeroFilledTopNBinaryFn(
                Granularities.ALL,
                new DefaultDimensionSpec("testdim", null),
                new InvertedTopNMetricSpec(new NumericTopNMetricSpec("index")),
                3,
                aggregatorFactories,
                postAggregators,
                dimValues
        ).apply(
                result1,
                result2
        );
        Assert.assertEquals(expected.getTimestamp(), actual.getTimestamp());
        assertTopNMergeResult(expected.getValue(), actual.getValue());
    }

    @Test
    public void testMerge_When_NonInverted_Expect_ZeroFilledOne() {
        Result<TopNResultValue> result1 = new Result<TopNResultValue>(
                currTime,
                new TopNResultValue(
                        ImmutableList.<Map<String, Object>>of(
                                ImmutableMap.of(
                                        "rows", 1L,
                                        "index", 2L,
                                        "testdim", "11"
                                ),
                                ImmutableMap.of(
                                        "rows", 2L,
                                        "index", 4L,
                                        "testdim", "22"
                                )
                        )
                )
        );
        Result<TopNResultValue> result2 = new Result<TopNResultValue>(
                currTime,
                new TopNResultValue(
                        ImmutableList.<Map<String, Object>>of(
                                ImmutableMap.of(
                                        "rows", 2L,
                                        "index", 3L,
                                        "testdim", "22"
                                ),
                                ImmutableMap.of(
                                        "rows", 2L,
                                        "index", 0L,
                                        "testdim", "11"
                                )
                        )
                )
        );

        Result<TopNResultValue> expected = new Result<TopNResultValue>(
                currTime,
                new TopNResultValue(
                        ImmutableList.<Map<String, Object>>of(
                                ImmutableMap.of(
                                        "testdim", "22",
                                        "rows", 4L,
                                        "index", 7L
                                ),
                                ImmutableMap.of(
                                        "testdim", "11",
                                        "rows", 3L,
                                        "index", 2L
                                ),
                                ImmutableMap.of(
                                        "testdim", "1",
                                        "rows", 0L,
                                        "index", 0L
                                )
                        )
                )
        );

        Result<TopNResultValue> actual = new ZeroFilledTopNBinaryFn(
                Granularities.ALL,
                new DefaultDimensionSpec("testdim", null),
                new NumericTopNMetricSpec("index"),
                3,
                aggregatorFactories,
                postAggregators,
                dimValues
        ).apply(
                result1,
                result2
        );
        Assert.assertEquals(expected.getTimestamp(), actual.getTimestamp());
        assertTopNMergeResult(expected.getValue(), actual.getValue());
    }

}
