/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query.zerofilledtopn;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.dimension.LegacyDimensionSpec;
import org.apache.druid.query.topn.InvertedTopNMetricSpec;
import org.apache.druid.query.topn.NumericTopNMetricSpec;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ZeroFilledTopNQueryTest
{

  private static final ObjectMapper JSON_MAPPER = TestHelper.makeJsonMapper();

  @Before
  public void setup()
  {
    NullHandling.initializeForTests();
  }

  @Test(expected = IAE.class)
  public void testZeroFilledTopNQuery_When_DimValuesNotSet_Expect_IAE()
  {
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
  public void testZeroFilledTopNQuery_When_ContextIsNull_Expect_NPE()
  {
    new ZeroFilledTopNQueryBuilder()
        .dataSource("test_ds")
        .intervals("2018-10-28T00:00:00.000-05:00/2018-10-29T00:00:00.000-05:00")
        .metric("test_metric")
        .build();
  }

  @Test
  public void testQuery_WithNumericTopNMetricSpec() throws IOException
  {
    Map<String, Object> context = new HashMap<>();
    context.put(ZeroFilledTopNQuery.DIM_VALUES, Lists.newArrayList("1", "2", "3"));

    ZeroFilledTopNQuery expectedQuery = new ZeroFilledTopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(new LegacyDimensionSpec(QueryRunnerTestHelper.MARKET_DIMENSION))
        .metric(new NumericTopNMetricSpec("rows"))
        .threshold(2)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC.getIntervals())
        .aggregators(Collections.singletonList(QueryRunnerTestHelper.ROWS_COUNT))
        .context(context)
        .build();
    String jsonQuery = "{\n"
                       + "  \"queryType\": \"zeroFilledTopN\",\n"
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
    ZeroFilledTopNQuery actualQuery = JSON_MAPPER.readValue(
        jsonQuery,
        ZeroFilledTopNQuery.class
    );
    Assert.assertEquals(expectedQuery.toString(), actualQuery.toString());
  }

  @Test
  public void testQuery_WithInvertedNumericTopNMetricSpec() throws IOException
  {
    Map<String, Object> context = new HashMap<>();
    context.put(ZeroFilledTopNQuery.DIM_VALUES, Lists.newArrayList("1", "2", "3"));

    ZeroFilledTopNQuery expectedQuery = new ZeroFilledTopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(new LegacyDimensionSpec(QueryRunnerTestHelper.MARKET_DIMENSION))
        .metric(new InvertedTopNMetricSpec(new NumericTopNMetricSpec("rows")))
        .threshold(2)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC.getIntervals())
        .aggregators(Collections.singletonList(QueryRunnerTestHelper.ROWS_COUNT))
        .context(context)
        .build();
    String jsonQuery = "{\n"
                       + "  \"queryType\": \"zeroFilledTopN\",\n"
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
    ZeroFilledTopNQuery actualQuery = JSON_MAPPER.readValue(
        JSON_MAPPER.writeValueAsString(JSON_MAPPER.readValue(
            jsonQuery,
            ZeroFilledTopNQuery.class
        )),
        ZeroFilledTopNQuery.class
    );

    Assert.assertEquals(expectedQuery.toString(), actualQuery.toString());
  }

  @Test
  public void testQuery_WithNumericTopNMetricSpec_AndDimOutputName() throws IOException
  {
    Map<String, Object> context = new HashMap<>();
    context.put(ZeroFilledTopNQuery.DIM_VALUES, Lists.newArrayList("1", "2", "3"));
    ZeroFilledTopNQuery expectedQuery = new ZeroFilledTopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension("CAMPAIGN_PLACEMENT_ID", "placement")
        .metric(new NumericTopNMetricSpec("rows"))
        .threshold(2)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC.getIntervals())
        .aggregators(Collections.singletonList(QueryRunnerTestHelper.ROWS_COUNT))
        .context(context)
        .build();
    String jsonQuery = "{\n"
                       + "  \"queryType\": \"zeroFilledTopN\",\n"
                       + "  \"dataSource\": \"testing\",\n"
                       + "  \"dimension\":{\"dimension\":\"CAMPAIGN_PLACEMENT_ID\",\"outputName\":\"placement\",\"type\":\"default\"}, \n"
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
    ZeroFilledTopNQuery actualQuery = JSON_MAPPER.readValue(
        jsonQuery,
        ZeroFilledTopNQuery.class
    );
    Assert.assertEquals(expectedQuery.toString(), actualQuery.toString());
  }

}
