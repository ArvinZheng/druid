package org.apache.druid.query.zerofilledtopn;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.query.DefaultQueryMetrics;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.topn.DefaultTopNQueryMetrics;
import org.apache.druid.query.topn.TopNQueryMetrics;

public class DefaultZeroFilledTopNQueryMetrics extends DefaultQueryMetrics<ZeroFilledTopNQuery> implements ZeroFilledTopNQueryMetrics {

    public DefaultZeroFilledTopNQueryMetrics(ObjectMapper jsonMapper) {
        super(jsonMapper);
    }

    @Override
    public void query(ZeroFilledTopNQuery query) {
        super.query(query);
        threshold(query);
        dimension(query);
        numMetrics(query);
        numComplexMetrics(query);
    }

    @Override
    public void threshold(ZeroFilledTopNQuery query) {
        setDimension("threshold", String.valueOf(query.getThreshold()));
    }

    @Override
    public void dimension(ZeroFilledTopNQuery query) {
        setDimension("dimension", query.getDimensionSpec().getDimension());
    }

    @Override
    public void numMetrics(ZeroFilledTopNQuery query) {
        setDimension("numMetrics", String.valueOf(query.getAggregatorSpecs().size()));
    }

    @Override
    public void numComplexMetrics(ZeroFilledTopNQuery query) {
        int numComplexAggs = DruidMetrics.findNumComplexAggs(query.getAggregatorSpecs());
        setDimension("numComplexMetrics", String.valueOf(numComplexAggs));
    }

    @Override
    public TopNQueryMetrics toTopNQueryMetrics(ZeroFilledTopNQuery zeroFilledTopNQuery) {
        TopNQueryMetrics topNQueryMetrics = new DefaultTopNQueryMetrics(this.jsonMapper);
        topNQueryMetrics.query(zeroFilledTopNQuery.toTopNQuery());
        return topNQueryMetrics;
    }

}
