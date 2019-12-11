package org.apache.druid.query.zerofilledtopn;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.jackson.DefaultObjectMapper;

@LazySingleton
public class DefaultZeroFilledTopNQueryMetricsFactory implements ZeroFilledTopNQueryMetricsFactory {

    private static final ZeroFilledTopNQueryMetricsFactory INSTANCE = new DefaultZeroFilledTopNQueryMetricsFactory(new DefaultObjectMapper());

    @VisibleForTesting
    public static ZeroFilledTopNQueryMetricsFactory instance()
    {
        return INSTANCE;
    }

    private final ObjectMapper jsonMapper;

    @Inject
    public DefaultZeroFilledTopNQueryMetricsFactory(@Json ObjectMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }

    @Override
    public ZeroFilledTopNQueryMetrics makeMetrics() {
        return new DefaultZeroFilledTopNQueryMetrics(jsonMapper);
    }

}
