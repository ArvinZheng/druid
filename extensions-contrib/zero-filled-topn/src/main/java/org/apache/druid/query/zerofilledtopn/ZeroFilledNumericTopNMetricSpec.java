package org.apache.druid.query.zerofilledtopn;

import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.topn.NumericTopNMetricSpec;
import org.apache.druid.query.topn.TopNResultBuilder;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.List;
import java.util.Set;

public class ZeroFilledNumericTopNMetricSpec extends AbstractZeroFilledTopNMetricSpec {
    // use the MAX_VALUE to avoid potential collision
    private static final byte CACHE_TYPE_ID = Byte.MAX_VALUE;

    public ZeroFilledNumericTopNMetricSpec(NumericTopNMetricSpec delegate) {
        super(delegate);
    }

    @Override
    public TopNResultBuilder getResultBuilder(DateTime timestamp, DimensionSpec dimSpec, int threshold, Comparator comparator, List<AggregatorFactory> aggFactories, List<PostAggregator> postAggs, Set<Comparable> dimValues) {
        return new ZeroFilledTopNNumericResultBuilder(timestamp, dimSpec, getMetricName(dimSpec), threshold, comparator, aggFactories, postAggs, dimValues, false);
    }

    @Override
    public byte getCacheTypeId() {
        return CACHE_TYPE_ID;
    }

    @Override
    public Comparator getComparator(List<AggregatorFactory> aggregatorSpecs, List<PostAggregator> postAggregatorSpecs) {
        return getDelegate().getComparator(aggregatorSpecs, postAggregatorSpecs);
    }

}
