package org.apache.druid.query.zerofilledtopn;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.topn.NumericTopNMetricSpec;
import org.apache.druid.query.topn.TopNAlgorithmSelector;
import org.apache.druid.query.topn.TopNMetricSpec;
import org.apache.druid.query.topn.TopNMetricSpecBuilder;
import org.apache.druid.query.topn.TopNResultBuilder;
import org.joda.time.DateTime;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public abstract class AbstractZeroFilledTopNMetricSpec implements TopNMetricSpec {

    private final NumericTopNMetricSpec delegate;

    @JsonCreator
    public AbstractZeroFilledTopNMetricSpec(@JsonProperty("metric") NumericTopNMetricSpec delegate) {
        this.delegate = delegate;
    }

    @Override
    public void verifyPreconditions(List<AggregatorFactory> aggregatorSpecs, List<PostAggregator> postAggregatorSpecs) {
        delegate.verifyPreconditions(aggregatorSpecs, postAggregatorSpecs);
    }

    public TopNMetricSpec getDelegate() {
        return delegate;
    }

    @JsonProperty("metric")
    public String getMetric() {
        return delegate.getMetric();
    }

    @Override
    public TopNResultBuilder getResultBuilder(
            DateTime timestamp,
            DimensionSpec dimSpec,
            int threshold,
            Comparator comparator,
            List<AggregatorFactory> aggFactories,
            List<PostAggregator> postAggs
    ) {
        return this.delegate.getResultBuilder(
                timestamp,
                dimSpec,
                threshold,
                comparator,
                aggFactories,
                postAggs);
    }

    public abstract TopNResultBuilder getResultBuilder(
            DateTime timestamp,
            DimensionSpec dimSpec,
            int threshold,
            Comparator comparator,
            List<AggregatorFactory> aggFactories,
            List<PostAggregator> postAggs,
            Set<Comparable> dimValues
    );

    @Override
    public byte[] getCacheKey() {
        final byte[] cacheKey = delegate.getCacheKey();

        return ByteBuffer.allocate(1 + cacheKey.length)
                .put(getCacheTypeId())
                .put(cacheKey)
                .array();
    }

    public abstract byte getCacheTypeId();

    @Override
    public <T> TopNMetricSpecBuilder<T> configureOptimizer(TopNMetricSpecBuilder<T> builder) {
        if (!canBeOptimizedUnordered()) {
            return builder;
        }
        return delegate.configureOptimizer(builder);
    }

    @Override
    public void initTopNAlgorithmSelector(TopNAlgorithmSelector selector) {
        delegate.initTopNAlgorithmSelector(selector);
    }

    @Override
    public String getMetricName(DimensionSpec dimSpec) {
        return delegate.getMetric();
    }

    @Override
    public boolean canBeOptimizedUnordered() {
        return delegate.canBeOptimizedUnordered();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AbstractZeroFilledTopNMetricSpec that = (AbstractZeroFilledTopNMetricSpec) o;
        return Objects.equals(delegate, that.delegate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(delegate);
    }

    @Override
    public String toString() {
        return "AbstractZeroFilledTopNMetricSpec{" +
                "delegate=" + delegate +
                '}';
    }

}
