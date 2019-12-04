package org.apache.druid.query.zerofilledtopn;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.query.DefaultQueryMetrics;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.topn.TopNAlgorithm;
import org.apache.druid.query.topn.TopNParams;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;

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
        granularity(query);
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
    public void granularity(ZeroFilledTopNQuery query) {
        // Don't emit by default
    }

    @Override
    public void dimensionCardinality(int cardinality) {
        // Don't emit by default.
    }

    @Override
    public void algorithm(TopNAlgorithm algorithm) {
        // Emit nothing by default.
    }

    @Override
    public void cursor(Cursor cursor) {
        // Emit nothing by default.
    }

    @Override
    public void columnValueSelector(ColumnValueSelector columnValueSelector) {
        // Emit nothing by default.
    }

    @Override
    public void numValuesPerPass(TopNParams params) {
        // Don't emit by default.
    }

    @Override
    public DefaultZeroFilledTopNQueryMetrics addProcessedRows(long numRows) {
        // Emit nothing by default.
        return this;
    }

    @Override
    public void startRecordingScanTime() {
        // Don't record scan time by default.
    }

    @Override
    public DefaultZeroFilledTopNQueryMetrics stopRecordingScanTime() {
        // Emit nothing by default.
        return this;
    }
}
