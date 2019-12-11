package org.apache.druid.query.zerofilledtopn;

import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.topn.TopNQueryMetrics;

public interface ZeroFilledTopNQueryMetrics extends QueryMetrics<ZeroFilledTopNQuery> {

    void threshold(ZeroFilledTopNQuery query);

    void dimension(ZeroFilledTopNQuery query);

    void numMetrics(ZeroFilledTopNQuery query);

    void numComplexMetrics(ZeroFilledTopNQuery query);

    public TopNQueryMetrics toTopNQueryMetrics(ZeroFilledTopNQuery zeroFilledTopNQuery);

}
