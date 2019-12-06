package org.apache.druid.query.zerofilledtopn;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.dimension.LegacyDimensionSpec;
import org.apache.druid.query.topn.InvertedTopNMetricSpec;
import org.apache.druid.query.topn.NumericTopNMetricSpec;
import org.apache.druid.query.zerofilledtopn.utils.TestHelper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.druid.query.zerofilledtopn.utils.QueryRunnerTestHelper.allGran;
import static org.apache.druid.query.zerofilledtopn.utils.QueryRunnerTestHelper.dataSource;
import static org.apache.druid.query.zerofilledtopn.utils.QueryRunnerTestHelper.fullOnIntervalSpec;
import static org.apache.druid.query.zerofilledtopn.utils.QueryRunnerTestHelper.marketDimension;
import static org.apache.druid.query.zerofilledtopn.utils.QueryRunnerTestHelper.rowsCount;

public class ZeroFilledTopNQueryTest {

    private static final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();

    @Test(expected = IAE.class)
    public void testZeroFilledTopNQuery_When_DimValuesNotSet_Expect_IAE() {
        Map<String, Object> context = new HashMap<>();
        new ZeroFilledTopNQueryBuilder()
                .context(context)
                .dataSource("test_ds")
                .dimension("test_dim")
                .intervals("2018-10-28T00:00:00.000-05:00/2018-10-29T00:00:00.000-05:00")
                .metric("test_metric")
                .build();
    }

    @Test(expected = NullPointerException.class)
    public void testZeroFilledTopNQuery_When_ContextIsNull_Expect_NPE() {
        new ZeroFilledTopNQueryBuilder()
                .dataSource("test_ds")
                .intervals("2018-10-28T00:00:00.000-05:00/2018-10-29T00:00:00.000-05:00")
                .metric("test_metric")
                .build();
    }

    @Test
    public void testQuery_WithNumericTopNMetricSpec() throws IOException {
        Map<String, Object> context = new HashMap<>();
        context.put(ZeroFilledTopNQuery.DIM_VALUES, Lists.newArrayList("1", "2", "3"));
        ZeroFilledTopNQuery expectedQuery = new ZeroFilledTopNQueryBuilder()
                .dataSource(dataSource)
                .granularity(allGran)
                .dimension(new LegacyDimensionSpec(marketDimension))
                .metric(new NumericTopNMetricSpec("rows"))
                .threshold(2)
                .intervals(fullOnIntervalSpec.getIntervals())
                .aggregators(Collections.singletonList(rowsCount))
                .context(context)
                .build();
        String jsonQuery = "{\n"
                + "  \"queryType\": \"ZeroFilledTopNQuery\",\n"
                + "  \"dataSource\": \"testing\",\n"
                + "  \"dimension\": \"market\",\n"
                + "  \"threshold\": 2,\n"
                + "  \"metric\": \"rows\",\n"
                + "  \"granularity\": \"all\",\n"
                + "  \"aggregations\": [\n"
                + "    {\n"
                + "      \"type\": \"count\",\n"
                + "      \"name\": \"rows\"\n"
                + "    }\n"
                + "  ],\n"
                + "  \"intervals\": [\n"
                + "    \"1970-01-01T00:00:00.000Z/2020-01-01T00:00:00.000Z\"\n"
                + "  ],\n"
                + " \"context\": {\n"
                + "    \"zeroFilledDimValues\": [\"1\", \"2\", \"3\"]\n"
                + "   }"
                + "}";
        ZeroFilledTopNQuery actualQuery = jsonMapper.readValue(
                jsonQuery,
                ZeroFilledTopNQuery.class
        );
        Assert.assertEquals(expectedQuery.toString(), actualQuery.toString());
    }

    @Test
    public void testQuery_WithInvertedNumericTopNMetricSpec() throws IOException {
        Map<String, Object> context = new HashMap<>();
        context.put(ZeroFilledTopNQuery.DIM_VALUES, Lists.newArrayList("1", "2", "3"));
        ZeroFilledTopNQuery expectedQuery = new ZeroFilledTopNQueryBuilder()
                .dataSource(dataSource)
                .granularity(allGran)
                .dimension(new LegacyDimensionSpec(marketDimension))
                .metric(new InvertedTopNMetricSpec(new NumericTopNMetricSpec("rows")))
                .threshold(2)
                .intervals(fullOnIntervalSpec.getIntervals())
                .aggregators(Collections.singletonList(rowsCount))
                .context(context)
                .build();
        String jsonQuery = "{\n"
                + "  \"queryType\": \"ZeroFilledTopNQuery\",\n"
                + "  \"dataSource\": \"testing\",\n"
                + "  \"dimension\": \"market\",\n"
                + "  \"threshold\": 2,\n"
                + "  \"metric\": {\"type\": \"inverted\", \"metric\": \"rows\"},\n"
                + "  \"granularity\": \"all\",\n"
                + "  \"aggregations\": [\n"
                + "    {\n"
                + "      \"type\": \"count\",\n"
                + "      \"name\": \"rows\"\n"
                + "    }\n"
                + "  ],\n"
                + "  \"intervals\": [\n"
                + "    \"1970-01-01T00:00:00.000Z/2020-01-01T00:00:00.000Z\"\n"
                + "  ],\n"
                + " \"context\": {\n"
                + "    \"zeroFilledDimValues\": [\"1\", \"2\", \"3\"]\n"
                + "   }"
                + "}";
        ZeroFilledTopNQuery actualQuery = jsonMapper.readValue(
                jsonQuery,
                ZeroFilledTopNQuery.class
        );
        Assert.assertEquals(expectedQuery.toString(), actualQuery.toString());
    }

}
