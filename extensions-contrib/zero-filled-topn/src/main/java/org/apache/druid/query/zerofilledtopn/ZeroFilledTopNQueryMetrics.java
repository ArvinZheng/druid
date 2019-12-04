package org.apache.druid.query.zerofilledtopn;

import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.topn.TopNAlgorithm;
import org.apache.druid.query.topn.TopNParams;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;

public interface ZeroFilledTopNQueryMetrics extends QueryMetrics<ZeroFilledTopNQuery> {

    @PublicApi
    void threshold(ZeroFilledTopNQuery query);

    @PublicApi
    void dimension(ZeroFilledTopNQuery query);

    @PublicApi
    void numMetrics(ZeroFilledTopNQuery query);

    @PublicApi
    void numComplexMetrics(ZeroFilledTopNQuery query);

    @PublicApi
    void granularity(ZeroFilledTopNQuery query);

    @PublicApi
    void dimensionCardinality(int cardinality);

    @PublicApi
    void algorithm(TopNAlgorithm algorithm);

    @PublicApi
    void cursor(Cursor cursor);

    @PublicApi
    void columnValueSelector(ColumnValueSelector columnValueSelector);

    @PublicApi
    void numValuesPerPass(TopNParams params);

    @PublicApi
    ZeroFilledTopNQueryMetrics addProcessedRows(long numRows);

    void startRecordingScanTime();

    ZeroFilledTopNQueryMetrics stopRecordingScanTime();

}
