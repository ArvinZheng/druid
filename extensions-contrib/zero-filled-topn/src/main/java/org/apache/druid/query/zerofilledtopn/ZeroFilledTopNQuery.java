package org.apache.druid.query.zerofilledtopn;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.PerSegmentQueryOptimizationContext;
import org.apache.druid.query.Queries;
import org.apache.druid.query.Query;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.query.topn.InvertedTopNMetricSpec;
import org.apache.druid.query.topn.NumericTopNMetricSpec;
import org.apache.druid.query.topn.TopNAlgorithmSelector;
import org.apache.druid.query.topn.TopNMetricSpec;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.query.topn.TopNResultValue;
import org.apache.druid.segment.VirtualColumns;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A special TopN query which takes a list of values as topN dimension, and do zero-filling for the input dimension values when needed
 * NOTE: ZeroFilledTopNQuery implicitly creates an InDimFilter by the zeroFilledDimValues in context and adds it to the query filter by And connection
 */
public class ZeroFilledTopNQuery extends BaseQuery<Result<TopNResultValue>> {

    public static final String ZERO_FILLED_TOPN = "ZeroFilledTopNQuery";
    public static final String DIM_VALUES = "zeroFilledDimValues";
    private final Set<String> dimValues;

    private final VirtualColumns virtualColumns;
    private final DimensionSpec dimensionSpec;
    private final AbstractZeroFilledTopNMetricSpec topNMetricSpec;
    private final int threshold;
    private final DimFilter dimFilter;
    private final List<AggregatorFactory> aggregatorSpecs;
    private final List<PostAggregator> postAggregatorSpecs;

    @JsonCreator
    public ZeroFilledTopNQuery(
            @JsonProperty("dataSource") DataSource dataSource,
            @JsonProperty("virtualColumns") VirtualColumns virtualColumns,
            @JsonProperty("dimension") DimensionSpec dimensionSpec,
            @JsonProperty("metric") TopNMetricSpec topNMetricSpec,
            @JsonProperty("threshold") int threshold,
            @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
            @JsonProperty("filter") DimFilter dimFilter,
            @JsonProperty("granularity") Granularity granularity,
            @JsonProperty("aggregations") List<AggregatorFactory> aggregatorSpecs,
            @JsonProperty("postAggregations") List<PostAggregator> postAggregatorSpecs,
            @JsonProperty("context") Map<String, Object> context
    ) {
        super(dataSource, querySegmentSpec, false, context, granularity);

        Preconditions.checkNotNull(context, "context must not be null for " + ZERO_FILLED_TOPN);

        this.virtualColumns = VirtualColumns.nullToEmpty(virtualColumns);
        this.dimensionSpec = dimensionSpec;
        this.topNMetricSpec = convertTopNMetricSpec(topNMetricSpec);
        this.threshold = threshold;

        this.dimValues = new HashSet<>(extractDimensionValues(context));
        this.dimFilter = resolveFilter(dimFilter, this.dimValues, dimensionSpec.getDimension());
        this.aggregatorSpecs = aggregatorSpecs == null ? ImmutableList.of() : aggregatorSpecs;
        this.postAggregatorSpecs = Queries.prepareAggregations(
                ImmutableList.of(dimensionSpec.getOutputName()),
                this.aggregatorSpecs,
                postAggregatorSpecs == null
                        ? ImmutableList.of()
                        : postAggregatorSpecs
        );

        Preconditions.checkNotNull(dimensionSpec, "dimensionSpec can't be null");
        Preconditions.checkNotNull(topNMetricSpec, "must specify a metric");
        Preconditions.checkArgument(threshold != 0, "Threshold cannot be equal to 0.");

        topNMetricSpec.verifyPreconditions(this.aggregatorSpecs, this.postAggregatorSpecs);
    }

    private AbstractZeroFilledTopNMetricSpec convertTopNMetricSpec(TopNMetricSpec topNMetricSpec) {

        if (topNMetricSpec instanceof AbstractZeroFilledTopNMetricSpec) {
            return (AbstractZeroFilledTopNMetricSpec) topNMetricSpec;
        }

        AbstractZeroFilledTopNMetricSpec zeroFilledTopNMetricSpec;

        if (topNMetricSpec instanceof NumericTopNMetricSpec) {
            zeroFilledTopNMetricSpec = new ZeroFilledNumericTopNMetricSpec(topNMetricSpec);
        } else if (topNMetricSpec instanceof InvertedTopNMetricSpec) {
            InvertedTopNMetricSpec invertedTopNMetricSpec = (InvertedTopNMetricSpec) topNMetricSpec;
            if (!(invertedTopNMetricSpec.getDelegate() instanceof NumericTopNMetricSpec)) {
                throw new IAE("zeroFilledTopNQuery currently supports only NumericTopNMetricSpec, but get %s", topNMetricSpec);
            } else {
                zeroFilledTopNMetricSpec = new InvertedZeroFilledNumericTopNMetricSpec(invertedTopNMetricSpec.getDelegate());
            }
        } else {
            throw new IAE("zeroFilledTopNQuery currently supports only NumericTopNMetricSpec, but get %s", topNMetricSpec);
        }

        return zeroFilledTopNMetricSpec;
    }

    private List<String> extractDimensionValues(Map<String, Object> context) {
        List<String> ret = (ArrayList<String>) context.get(DIM_VALUES);
        if (ret == null || ret.isEmpty()) {
            throw new IAE("zeroFilledDimValues is required for zeroFilledTopNQuery");
        }
        return ret;
    }

    /**
     * This method implicitly creates an InDimFilter by the zeroFilledDimValues in context and adds it to the query filter by And connection, i.e. dimFilter And dimValueFilter
     *
     * @param dimFilter original dimension filter
     * @param dimValues dimension values for zero-filled
     * @param dimension
     * @return resolved DimFilter which connects the original filter (if exists) and the implicitly created InDimFilter by And connection
     */
    private DimFilter resolveFilter(DimFilter dimFilter, Set<String> dimValues, String dimension) {
        InDimFilter dimValueFilter = new InDimFilter(dimension, dimValues, null);
        DimFilter resolvedFilter;
        if (dimFilter == null) {
            resolvedFilter = dimValueFilter;
        } else {
            resolvedFilter = new AndDimFilter(dimFilter, dimValueFilter);
        }
        return resolvedFilter;
    }

    @Override
    public boolean hasFilters() {
        return dimFilter != null;
    }

    @Override
    public DimFilter getFilter() {
        return dimFilter;
    }

    @Override
    public String getType() {
        return ZERO_FILLED_TOPN;
    }

    @JsonProperty
    public VirtualColumns getVirtualColumns() {
        return virtualColumns;
    }

    @JsonProperty("dimension")
    public DimensionSpec getDimensionSpec() {
        return dimensionSpec;
    }

    @JsonProperty("metric")
    public AbstractZeroFilledTopNMetricSpec getTopNMetricSpec() {
        return topNMetricSpec;
    }

    @JsonProperty("threshold")
    public int getThreshold() {
        return threshold;
    }

    @JsonProperty("filter")
    public DimFilter getDimensionsFilter() {
        return dimFilter;
    }

    @JsonProperty("aggregations")
    public List<AggregatorFactory> getAggregatorSpecs() {
        return aggregatorSpecs;
    }

    @JsonProperty("postAggregations")
    public List<PostAggregator> getPostAggregatorSpecs() {
        return postAggregatorSpecs;
    }

    public Set<String> getDimValues() {
        return dimValues;
    }

    public void initTopNAlgorithmSelector(TopNAlgorithmSelector selector) {
        if (dimensionSpec.getExtractionFn() != null) {
            selector.setHasExtractionFn(true);
        }
        topNMetricSpec.initTopNAlgorithmSelector(selector);
    }

    @Override
    public ZeroFilledTopNQuery withQuerySegmentSpec(QuerySegmentSpec querySegmentSpec) {
        return new ZeroFilledTopNQueryBuilder(this).intervals(querySegmentSpec).build();
    }

    public ZeroFilledTopNQuery withDimensionSpec(DimensionSpec spec) {
        return new ZeroFilledTopNQueryBuilder(this).dimension(spec).build();
    }

    public ZeroFilledTopNQuery withAggregatorSpecs(List<AggregatorFactory> aggregatorSpecs) {
        return new ZeroFilledTopNQueryBuilder(this).aggregators(aggregatorSpecs).build();
    }

    @Override
    public Query<Result<TopNResultValue>> withDataSource(DataSource dataSource) {
        return new ZeroFilledTopNQueryBuilder(this).dataSource(dataSource).build();
    }

    @Override
    public Query<Result<TopNResultValue>> optimizeForSegment(PerSegmentQueryOptimizationContext optimizationContext) {
        return new ZeroFilledTopNQueryBuilder(this).aggregators(optimizeAggs(optimizationContext)).build();
    }

    public ZeroFilledTopNQuery withThreshold(int threshold) {
        return new ZeroFilledTopNQueryBuilder(this).threshold(threshold).build();
    }

    @Override
    public ZeroFilledTopNQuery withOverriddenContext(Map<String, Object> contextOverrides) {
        return new ZeroFilledTopNQueryBuilder(this).context(computeOverriddenContext(getContext(), contextOverrides)).build();
    }

    public ZeroFilledTopNQuery withDimFilter(DimFilter dimFilter) {
        return new ZeroFilledTopNQueryBuilder(this).filters(dimFilter).build();
    }

    /**
     * in order to reuse existing TopN query engines, convert ZeroFilledTopNQuery to TopNQuery
     *
     * @return TopNQuery
     */
    public TopNQuery toTopNQuery() {
        return new TopNQueryBuilder()
                .dataSource(this.getDataSource())
                .virtualColumns(virtualColumns)
                .dimension(dimensionSpec)
                .metric(topNMetricSpec)
                .threshold(threshold)
                .intervals(this.getQuerySegmentSpec())
                .filters(dimFilter)
                .granularity(this.getGranularity())
                .aggregators(aggregatorSpecs)
                .postAggregators(postAggregatorSpecs)
                .context(this.getContext())
                .build();
    }

    @Override
    public String toString() {
        return "ZeroFilledTopNQuery{" +
                "dimValues=" + dimValues +
                ", virtualColumns=" + virtualColumns +
                ", dimensionSpec=" + dimensionSpec +
                ", topNMetricSpec=" + topNMetricSpec +
                ", threshold=" + threshold +
                ", dimFilter=" + dimFilter +
                ", aggregatorSpecs=" + aggregatorSpecs +
                ", postAggregatorSpecs=" + postAggregatorSpecs +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        ZeroFilledTopNQuery that = (ZeroFilledTopNQuery) o;
        return threshold == that.threshold &&
                Objects.equals(dimValues, that.dimValues) &&
                Objects.equals(virtualColumns, that.virtualColumns) &&
                Objects.equals(dimensionSpec, that.dimensionSpec) &&
                Objects.equals(topNMetricSpec, that.topNMetricSpec) &&
                Objects.equals(dimFilter, that.dimFilter) &&
                Objects.equals(aggregatorSpecs, that.aggregatorSpecs) &&
                Objects.equals(postAggregatorSpecs, that.postAggregatorSpecs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), dimValues, virtualColumns, dimensionSpec, topNMetricSpec, threshold, dimFilter, aggregatorSpecs, postAggregatorSpecs);
    }

    private List<AggregatorFactory> optimizeAggs(PerSegmentQueryOptimizationContext optimizationContext) {
        List<AggregatorFactory> optimizedAggs = new ArrayList<>();
        for (AggregatorFactory aggregatorFactory : aggregatorSpecs) {
            optimizedAggs.add(aggregatorFactory.optimizeForSegment(optimizationContext));
        }
        return optimizedAggs;
    }

}
