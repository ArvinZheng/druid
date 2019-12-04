package org.apache.druid.query.zerofilledtopn;

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.granularity.AllGranularity;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.nary.BinaryFn;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.topn.DimensionAndMetricValueExtractor;
import org.apache.druid.query.topn.NumericTopNMetricSpec;
import org.apache.druid.query.topn.TopNResultBuilder;
import org.apache.druid.query.topn.TopNResultValue;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.ValueType;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ZeroFilledTopNBinaryFn implements BinaryFn<Result<TopNResultValue>, Result<TopNResultValue>, Result<TopNResultValue>> {

    private final DimensionSpec dimSpec;
    private final Granularity gran;
    private final String dimension;
    private final AbstractZeroFilledTopNMetricSpec topNMetricSpec;
    private final int threshold;
    private final List<AggregatorFactory> aggregations;
    private final List<PostAggregator> postAggregations;
    private final Comparator comparator;
    private final Set<String> dimValues;

    public ZeroFilledTopNBinaryFn(
            final Granularity granularity,
            final DimensionSpec dimSpec,
            final AbstractZeroFilledTopNMetricSpec topNMetricSpec,
            final int threshold,
            final List<AggregatorFactory> aggregatorSpecs,
            final List<PostAggregator> postAggregatorSpecs,
            final Set<String> dimValues
    ) {
        this.dimSpec = dimSpec;
        this.gran = granularity;
        this.topNMetricSpec = checkTopNMetricSpec(topNMetricSpec);
        this.threshold = threshold;
        this.aggregations = aggregatorSpecs;

        this.postAggregations = AggregatorUtil.pruneDependentPostAgg(
                postAggregatorSpecs,
                topNMetricSpec.getMetricName(dimSpec)
        );

        this.dimension = dimSpec.getOutputName();
        this.comparator = topNMetricSpec.getComparator(aggregatorSpecs, postAggregatorSpecs);
        this.dimValues = dimValues;
    }

    private AbstractZeroFilledTopNMetricSpec checkTopNMetricSpec(AbstractZeroFilledTopNMetricSpec topNMetricSpec) {

        if (!(topNMetricSpec.getDelegate() instanceof NumericTopNMetricSpec)) {
            throw new IAE("zeroFilledTopNQuery currently supports only NumericTopNMetricSpec, but get %s", topNMetricSpec);
        }

        return topNMetricSpec;
    }

    @Override
    public Result<TopNResultValue> apply(Result<TopNResultValue> arg1, Result<TopNResultValue> arg2) {
        if (arg1 == null) {
            return arg2;
        }
        if (arg2 == null) {
            return arg1;
        }

        Map<Object, DimensionAndMetricValueExtractor> retVals = new LinkedHashMap<>();

        TopNResultValue arg1Vals = arg1.getValue();
        TopNResultValue arg2Vals = arg2.getValue();

        for (DimensionAndMetricValueExtractor arg1Val : arg1Vals) {
            retVals.put(arg1Val.getDimensionValue(dimension), arg1Val);
        }
        for (DimensionAndMetricValueExtractor arg2Val : arg2Vals) {
            final Object dimensionValue = arg2Val.getDimensionValue(dimension);
            DimensionAndMetricValueExtractor arg1Val = retVals.get(dimensionValue);

            if (arg1Val != null) {
                // size of map = aggregator + topNDim + postAgg (If sorting is done on post agg field)
                Map<String, Object> retVal = new LinkedHashMap<>(aggregations.size() + 2);

                retVal.put(dimension, dimensionValue);
                for (AggregatorFactory factory : aggregations) {
                    final String metricName = factory.getName();
                    retVal.put(metricName, factory.combine(arg1Val.getMetric(metricName), arg2Val.getMetric(metricName)));
                }

                for (PostAggregator pf : postAggregations) {
                    retVal.put(pf.getName(), pf.compute(retVal));
                }

                retVals.put(dimensionValue, new DimensionAndMetricValueExtractor(retVal));
            } else {
                retVals.put(dimensionValue, arg2Val);
            }
        }

        final DateTime timestamp;
        if (gran instanceof AllGranularity) {
            timestamp = arg1.getTimestamp();
        } else {
            timestamp = gran.bucketStart(arg1.getTimestamp());
        }

        TopNResultBuilder bob = topNMetricSpec.getResultBuilder(
                timestamp,
                dimSpec,
                threshold,
                comparator,
                aggregations,
                postAggregations,
                getConvertDimValues()
        );
        for (DimensionAndMetricValueExtractor extractor : retVals.values()) {
            bob.addEntry(extractor);
        }
        return bob.build();
    }

    private Set<Comparable> getConvertDimValues() {
        Set<Comparable> convertedDimValues = new HashSet<>();
        ValueType type = dimSpec.getOutputType();
        for (String value : dimValues) {
            convertedDimValues.add(DimensionHandlerUtils.convertObjectToType(value, type));
        }
        return convertedDimValues;
    }

}
