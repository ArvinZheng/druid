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

import java.util.Collections;
import java.util.List;

public class ZeroFilledTopNQueryModule implements DruidModule
{


  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.<Module>singletonList(new SimpleModule("ZeroFilledTopNQueryModule")
                                                 .registerSubtypes(new NamedType(
                                                     ZeroFilledTopNQuery.class,
                                                     ZeroFilledTopNQuery.ZERO_FILLED_TOPN
                                                 )));
  }

  @Override
  public void configure(Binder binder)
  {
    PolyBind.optionBinder(binder, Key.get(ZeroFilledTopNQueryMetricsFactory.class))
            .addBinding("zeroFilledTopNQueryMetricsFactory")
            .to(DefaultZeroFilledTopNQueryMetricsFactory.class);
    JsonConfigProvider.bindInstance(
        binder,
        Key.get(ZeroFilledTopNQueryMetricsFactory.class),
        DefaultZeroFilledTopNQueryMetricsFactory.instance()
    );

    MapBinder<Class<? extends Query>, QueryToolChest> toolChests = DruidBinders.queryToolChestBinder(binder);

    // Bind the query factory to the query class
    DruidBinders.queryRunnerFactoryBinder(binder)
                .addBinding(ZeroFilledTopNQuery.class)
                .to(ZeroFilledTopNQueryRunnerFactory.class);

    //Bind the query toolchest to the query class
    toolChests.addBinding(ZeroFilledTopNQuery.class).to(ZeroFilledTopNQueryQueryToolChest.class);

    //Bind the query toolchest to binder
    binder.bind(ZeroFilledTopNQueryQueryToolChest.class).in(LazySingleton.class);
  }

}
