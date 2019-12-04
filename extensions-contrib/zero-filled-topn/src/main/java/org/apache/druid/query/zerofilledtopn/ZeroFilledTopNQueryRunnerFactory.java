package org.apache.druid.query.zerofilledtopn;

import com.google.inject.Inject;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.ChainedExecutionQueryRunner;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.query.Result;
import org.apache.druid.query.topn.TopNQueryEngine;
import org.apache.druid.query.topn.TopNResultValue;
import org.apache.druid.segment.Segment;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class ZeroFilledTopNQueryRunnerFactory implements QueryRunnerFactory<Result<TopNResultValue>, ZeroFilledTopNQuery> {

    private final NonBlockingPool<ByteBuffer> computationBufferPool;
    private final ZeroFilledTopNQueryQueryToolChest toolchest;
    private final QueryWatcher queryWatcher;

    @Inject
    public ZeroFilledTopNQueryRunnerFactory(
            @Global NonBlockingPool<ByteBuffer> computationBufferPool,
            ZeroFilledTopNQueryQueryToolChest toolchest,
            QueryWatcher queryWatcher
    ) {
        this.computationBufferPool = computationBufferPool;
        this.toolchest = toolchest;
        this.queryWatcher = queryWatcher;
    }

    @Override
    public QueryRunner<Result<TopNResultValue>> createRunner(final Segment segment) {
        final TopNQueryEngine queryEngine = new TopNQueryEngine(computationBufferPool);
        return new QueryRunner<Result<TopNResultValue>>() {
            @Override
            public Sequence<Result<TopNResultValue>> run(
                    QueryPlus<Result<TopNResultValue>> input,
                    Map<String, Object> responseContext
            ) {
                if (!(input.getQuery() instanceof ZeroFilledTopNQuery)) {
                    throw new ISE("Got a [%s] which isn't a %s", input.getClass(), ZeroFilledTopNQuery.class);
                }

                ZeroFilledTopNQuery query = (ZeroFilledTopNQuery) input.getQuery();
                return queryEngine.query(query.toTopNQuery(), segment.asStorageAdapter(), null);
            }
        };

    }

    @Override
    public QueryRunner<Result<TopNResultValue>> mergeRunners(
            ExecutorService queryExecutor,
            Iterable<QueryRunner<Result<TopNResultValue>>> queryRunners
    ) {
        return new ChainedExecutionQueryRunner<>(queryExecutor, queryWatcher, queryRunners);
    }

    @Override
    public QueryToolChest<Result<TopNResultValue>, ZeroFilledTopNQuery> getToolchest() {
        return toolchest;
    }

}
