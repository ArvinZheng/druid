package org.apache.druid.query.zerofilledtopn;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.jackson.DefaultObjectMapper;

@LazySingleton
public class DefaultZeroFilledTopNQueryMetricsFactory implements ZeroFilledTopNQueryMetricsFactory {

    private static final DefaultZeroFilledTopNQueryMetricsFactory INSTANCE = new DefaultZeroFilledTopNQueryMetricsFactory(new DefaultObjectMapper());

    @VisibleForTesting
    public static DefaultZeroFilledTopNQueryMetricsFactory instance()
    {
        return INSTANCE;
    }

    private final ObjectMapper jsonMapper;

    @Inject
    public DefaultZeroFilledTopNQueryMetricsFactory(@Json ObjectMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }

    @Override
    public DefaultZeroFilledTopNQueryMetrics makeMetrics() {
        return new DefaultZeroFilledTopNQueryMetrics(jsonMapper);
    }

}
