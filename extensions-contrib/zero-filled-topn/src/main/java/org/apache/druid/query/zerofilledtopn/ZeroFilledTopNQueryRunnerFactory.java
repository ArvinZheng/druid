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

import com.google.inject.Inject;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.ChainedExecutionQueryRunner;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.query.Result;
import org.apache.druid.query.topn.TopNQueryEngine;
import org.apache.druid.query.topn.TopNQueryMetrics;
import org.apache.druid.query.topn.TopNResultValue;
import org.apache.druid.segment.Segment;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;

public class ZeroFilledTopNQueryRunnerFactory
    implements QueryRunnerFactory<Result<TopNResultValue>, ZeroFilledTopNQuery>
{

  private final NonBlockingPool<ByteBuffer> computationBufferPool;
  private final ZeroFilledTopNQueryQueryToolChest toolchest;
  private final QueryWatcher queryWatcher;

  @Inject
  public ZeroFilledTopNQueryRunnerFactory(
      @Global NonBlockingPool<ByteBuffer> computationBufferPool,
      ZeroFilledTopNQueryQueryToolChest toolchest,
      QueryWatcher queryWatcher
  )
  {
    this.computationBufferPool = computationBufferPool;
    this.toolchest = toolchest;
    this.queryWatcher = queryWatcher;
  }

  @Override
  public QueryRunner<Result<TopNResultValue>> createRunner(final Segment segment)
  {
    final TopNQueryEngine queryEngine = new TopNQueryEngine(computationBufferPool);
    return (input, responseContext) -> {
      if (!(input.getQuery() instanceof ZeroFilledTopNQuery)) {
        throw new ISE("Got a [%s] which isn't a %s", input.getClass(), ZeroFilledTopNQuery.class);
      }

      ZeroFilledTopNQuery query = (ZeroFilledTopNQuery) input.getQuery();
      TopNQueryMetrics queryMetrics = null;
      if (input.getQueryMetrics() != null && input.getQueryMetrics() instanceof ZeroFilledTopNQueryMetrics) {
        queryMetrics = ((ZeroFilledTopNQueryMetrics) input.getQueryMetrics()).toTopNQueryMetrics(query);
      }
      return queryEngine.query(query.toTopNQuery(), segment.asStorageAdapter(), queryMetrics);
    };

  }

  @Override
  public QueryRunner<Result<TopNResultValue>> mergeRunners(
      ExecutorService queryExecutor,
      Iterable<QueryRunner<Result<TopNResultValue>>> queryRunners
  )
  {
    return new ChainedExecutionQueryRunner<>(queryExecutor, queryWatcher, queryRunners);
  }

  @Override
  public QueryToolChest<Result<TopNResultValue>, ZeroFilledTopNQuery> getToolchest()
  {
    return toolchest;
  }

}
