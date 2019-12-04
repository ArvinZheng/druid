package org.apache.druid.query.zerofilledtopn;

import org.apache.druid.java.util.common.IAE;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ZeroFilledTopNQueryTest {

    @Test(expected = IAE.class)
    public void testZeroFilledTopNQuery_When_DimValuesNotSet_Expect_IAE() {
        Map<String, Object> context = new HashMap<>();
        new ZeroFilledTopNQueryBuilder()
                .context(context)
                .dataSource("test_ds")
                .intervals("2018-10-28T00:00:00.000-05:00/2018-10-29T00:00:00.000-05:00")
                .metric("test_metric")
                .build();
    }

}
