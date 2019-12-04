package org.apache.druid.query.zerofilledtopn;

import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.spec.LegacySegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.query.topn.NumericTopNMetricSpec;
import org.apache.druid.query.topn.TopNMetricSpec;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class ZeroFilledTopNQueryBuilder {

    private DataSource dataSource;
    private VirtualColumns virtualColumns;
    private DimensionSpec dimensionSpec;
    private TopNMetricSpec topNMetricSpec;
    private int threshold;
    private QuerySegmentSpec querySegmentSpec;
    private DimFilter dimFilter;
    private Granularity granularity;
    private List<AggregatorFactory> aggregatorSpecs;
    private List<PostAggregator> postAggregatorSpecs;
    private Map<String, Object> context;

    public ZeroFilledTopNQueryBuilder() {
        dataSource = null;
        virtualColumns = null;
        dimensionSpec = null;
        topNMetricSpec = null;
        threshold = 0;
        querySegmentSpec = null;
        dimFilter = null;
        granularity = Granularities.ALL;
        aggregatorSpecs = new ArrayList<>();
        postAggregatorSpecs = new ArrayList<>();
        context = null;
    }

    public ZeroFilledTopNQueryBuilder(final ZeroFilledTopNQuery query) {
        this.dataSource = query.getDataSource();
        this.virtualColumns = query.getVirtualColumns();
        this.dimensionSpec = query.getDimensionSpec();
        this.topNMetricSpec = query.getTopNMetricSpec();
        this.threshold = query.getThreshold();
        this.querySegmentSpec = query.getQuerySegmentSpec();
        this.dimFilter = query.getDimensionsFilter();
        this.granularity = query.getGranularity();
        this.aggregatorSpecs = query.getAggregatorSpecs();
        this.postAggregatorSpecs = query.getPostAggregatorSpecs();
        this.context = query.getContext();
    }

    public ZeroFilledTopNQuery build() {
        return new ZeroFilledTopNQuery(
                dataSource,
                virtualColumns,
                dimensionSpec,
                topNMetricSpec,
                threshold,
                querySegmentSpec,
                dimFilter,
                granularity,
                aggregatorSpecs,
                postAggregatorSpecs,
                context
        );
    }

    public ZeroFilledTopNQueryBuilder dataSource(String d) {
        dataSource = new TableDataSource(d);
        return this;
    }

    public ZeroFilledTopNQueryBuilder virtualColumns(VirtualColumns virtualColumns) {
        this.virtualColumns = virtualColumns;
        return this;
    }

    public ZeroFilledTopNQueryBuilder virtualColumns(VirtualColumn... virtualColumns) {
        return virtualColumns(VirtualColumns.create(Arrays.asList(virtualColumns)));
    }

    public ZeroFilledTopNQueryBuilder dataSource(DataSource d) {
        dataSource = d;
        return this;
    }

    public ZeroFilledTopNQueryBuilder dimension(String d) {
        return dimension(d, null);
    }

    public ZeroFilledTopNQueryBuilder dimension(String d, String outputName) {
        return dimension(new DefaultDimensionSpec(d, outputName));
    }

    public ZeroFilledTopNQueryBuilder dimension(DimensionSpec d) {
        dimensionSpec = d;
        return this;
    }

    public ZeroFilledTopNQueryBuilder metric(String s) {
        return metric(new ZeroFilledNumericTopNMetricSpec(new NumericTopNMetricSpec(s)));
    }

    public ZeroFilledTopNQueryBuilder metric(TopNMetricSpec t) {
        topNMetricSpec = t;
        return this;
    }

    public ZeroFilledTopNQueryBuilder threshold(int i) {
        threshold = i;
        return this;
    }

    public ZeroFilledTopNQueryBuilder intervals(QuerySegmentSpec q) {
        querySegmentSpec = q;
        return this;
    }

    public ZeroFilledTopNQueryBuilder intervals(String s) {
        querySegmentSpec = new LegacySegmentSpec(s);
        return this;
    }

    public ZeroFilledTopNQueryBuilder intervals(List<Interval> l) {
        querySegmentSpec = new LegacySegmentSpec(l);
        return this;
    }

    public ZeroFilledTopNQueryBuilder filters(String dimensionName, String value) {
        dimFilter = new SelectorDimFilter(dimensionName, value, null);
        return this;
    }

    public ZeroFilledTopNQueryBuilder filters(String dimensionName, String value, String... values) {
        dimFilter = new InDimFilter(dimensionName, Lists.asList(value, values), null);
        return this;
    }

    public ZeroFilledTopNQueryBuilder filters(DimFilter f) {
        dimFilter = f;
        return this;
    }

    public ZeroFilledTopNQueryBuilder granularity(Granularity g) {
        granularity = g;
        return this;
    }

    @SuppressWarnings("unchecked")
    public ZeroFilledTopNQueryBuilder aggregators(List<? extends AggregatorFactory> a) {
        aggregatorSpecs = new ArrayList<>(a); // defensive copy
        return this;
    }

    public ZeroFilledTopNQueryBuilder postAggregators(Collection<PostAggregator> p) {
        postAggregatorSpecs = new ArrayList<>(p); // defensive copy
        return this;
    }

    public ZeroFilledTopNQueryBuilder context(Map<String, Object> c) {
        context = c;
        return this;
    }

}
