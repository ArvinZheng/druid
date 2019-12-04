package org.apache.druid.query.zerofilledtopn;

import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.topn.TopNMetricSpec;
import org.apache.druid.query.topn.TopNResultBuilder;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.List;
import java.util.Set;

public class InvertedZeroFilledNumericTopNMetricSpec extends AbstractZeroFilledTopNMetricSpec {

    // use the MIN_VALUE to avoid potential collision
    private static final byte CACHE_TYPE_ID = Byte.MIN_VALUE;

    public InvertedZeroFilledNumericTopNMetricSpec(TopNMetricSpec delegate) {
        super(delegate);
    }

    @Override
    public TopNResultBuilder getResultBuilder(DateTime timestamp, DimensionSpec dimSpec, int threshold, Comparator comparator, List<AggregatorFactory> aggFactories, List<PostAggregator> postAggs, Set<Comparable> dimValues) {
        return new ZeroFilledTopNNumericResultBuilder(timestamp, dimSpec, getMetricName(dimSpec), threshold, comparator, aggFactories, postAggs, dimValues, true);
    }

    @Override
    public byte getCacheTypeId() {
        return CACHE_TYPE_ID;
    }

    @Override
    public Comparator getComparator(List<AggregatorFactory> aggregatorSpecs, List<PostAggregator> postAggregatorSpecs) {
        return Comparator.nullsFirst(getDelegate().getComparator(aggregatorSpecs, postAggregatorSpecs).reversed());
    }
}
