package org.apache.druid.query.zerofilledtopn;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.multibindings.MapBinder;
import org.apache.druid.guice.DruidBinders;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryToolChest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public class ZeroFilledTopNQueryModule implements DruidModule {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZeroFilledTopNQueryModule.class);

    @Override
    public List<? extends Module> getJacksonModules() {
        LOGGER.info("Registering ZeroFilledTopNQuery subtype");
        try {
            return Collections.<Module>singletonList(new SimpleModule("ZeroFilledTopNQueryModule")
                    .registerSubtypes(new NamedType(
                            ZeroFilledTopNQuery.class,
                            ZeroFilledTopNQuery.ZERO_FILLED_TOPN
                    )));
        } catch (Exception e) {
            LOGGER.error("Failed to register ZeroFilledTopNQuery subtype", e);
            throw e;
        }
    }

    @Override
    public void configure(Binder binder) {
        LOGGER.info("Configure ZeroFilledTopNQuery");
        try {
            PolyBind.optionBinder(binder, Key.get(ZeroFilledTopNQueryMetricsFactory.class))
                    .addBinding("zeroFilledTopNQueryMetricsFactory")
                    .to(DefaultZeroFilledTopNQueryMetricsFactory.class);
            JsonConfigProvider.bindInstance(binder, Key.get(ZeroFilledTopNQueryMetricsFactory.class), DefaultZeroFilledTopNQueryMetricsFactory.instance());

            MapBinder<Class<? extends Query>, QueryToolChest> toolChests = DruidBinders.queryToolChestBinder(binder);

            //Bind the query toolchest to the query class and add the binding to toolchest
            toolChests.addBinding(ZeroFilledTopNQuery.class).to(ZeroFilledTopNQueryQueryToolChest.class);

            //Bind the query toolchest to binder
            binder.bind(ZeroFilledTopNQueryQueryToolChest.class).in(LazySingleton.class);
        } catch (Exception e) {
            LOGGER.error("Failed to configure ZeroFilledTopNQuery", e);
            throw e;
        }
    }

}
