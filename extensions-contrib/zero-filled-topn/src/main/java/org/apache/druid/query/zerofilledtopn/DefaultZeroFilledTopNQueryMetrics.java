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

import org.apache.druid.query.DefaultQueryMetrics;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.topn.DefaultTopNQueryMetrics;
import org.apache.druid.query.topn.TopNQueryMetrics;

public class DefaultZeroFilledTopNQueryMetrics extends DefaultQueryMetrics<ZeroFilledTopNQuery>
    implements ZeroFilledTopNQueryMetrics
{

  @Override
  public void query(ZeroFilledTopNQuery query)
  {
    super.query(query);
    threshold(query);
    dimension(query);
    numMetrics(query);
    numComplexMetrics(query);
  }

  @Override
  public void threshold(ZeroFilledTopNQuery query)
  {
    setDimension("threshold", String.valueOf(query.getThreshold()));
  }

  @Override
  public void dimension(ZeroFilledTopNQuery query)
  {
    setDimension("dimension", query.getDimensionSpec().getDimension());
  }

  @Override
  public void numMetrics(ZeroFilledTopNQuery query)
  {
    setDimension("numMetrics", String.valueOf(query.getAggregatorSpecs().size()));
  }

  @Override
  public void numComplexMetrics(ZeroFilledTopNQuery query)
  {
    int numComplexAggs = DruidMetrics.findNumComplexAggs(query.getAggregatorSpecs());
    setDimension("numComplexMetrics", String.valueOf(numComplexAggs));
  }

  @Override
  public TopNQueryMetrics toTopNQueryMetrics(ZeroFilledTopNQuery zeroFilledTopNQuery)
  {
    TopNQueryMetrics topNQueryMetrics = new DefaultTopNQueryMetrics();
    topNQueryMetrics.query(zeroFilledTopNQuery.toTopNQuery());
    return topNQueryMetrics;
  }

}
