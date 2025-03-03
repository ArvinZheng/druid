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

package org.apache.druid.query.aggregation.hyperloglog;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.druid.error.DruidException;
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.hll.VersionZeroHyperLogLogCollector;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.NoopAggregator;
import org.apache.druid.query.aggregation.NoopBufferAggregator;
import org.apache.druid.query.aggregation.NoopVectorAggregator;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.TestColumnSelectorFactory;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.vector.TestVectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class HyperUniquesAggregatorFactoryTest
{
  static final HyperUniquesAggregatorFactory AGGREGATOR_FACTORY = new HyperUniquesAggregatorFactory(
      "hyperUnique",
      "uniques"
  );
  static final String V0_BASE64 = "AAYbEyQwFyQVASMCVFEQQgEQIxIhM4ISAQMhUkICEDFDIBMhMgFQFAFAMjAAEhEREyVAEiUBAhIjISATMCECMiERIRIiVRFRAyIAEgFCQSMEJAITATAAEAMQgCEBEjQiAyUTAyEQASJyAGURAAISAwISATETQhAREBYDIVIlFTASAzJgERIgRCcmUyAwNAMyEJMjIhQXQhEWECABQDETATEREjIRAgEyIiMxMBQiAkBBMDYAMEQQACMzMhIkMTQSkYIRABIBADMBAhIEISAENkEBQDAxETMAIEEwEzQiQSEVQSFBBAQDICIiAVIAMTAQIQYBIRABADMDEzEAQSMkEiAYFBAQI0AmECEyQSARRTIVMhEkMiKAMCUBxUghAkIBI3EmMAQiACEAJDJCAAADOzESEDBCRjMgEUQQETQwEWIhA6MlAiAAZDI1AgEIIDUyFDIHMQEEAwIRBRABBStCZCQhAgJSMQIiQEEURTBmM1MxACIAETGhMgQnBRICNiIREyIUNAEAAkABAwQSEBJBIhIhIRERAiIRACUhEUAVMkQGEVMjECYjACBwEQQSIRIgAAEyExQUFSEAIBJCIDIDYTAgMiNBIUADUiETADMoFEADETMCIwUEQkIAESMSIzIABDERIXEhIiACQgUSEgJiQCAUARIRAREDQiEUAkQgAgQiIEAzIxRCARIgBAAVAzMAECEwE0Qh8gAAASEhEiAiMhUxcRImIVABATYyUBAwIoE1QhRDIiYBIBEBEiQSQyERAAADMAARAEACFYUwQSQBIRIgURITARFSEzEHEBACOTMREBIAMjIgEhU0cxEQIRIhIi1wEgMRUBEgMQIRAnAVASURMHQBAiEyBSAAEBQTAWQ5EQA0IUMSISAUEiASIjIhMhMFJBBSEjEAECEwACASEQFBAjARITEQIgYTEKEAeAAiMkEyARowARFBAicRISIBIxAQAgEBARMCIRQgMSIVIAkjMxIAIEMyADASMgFRIjEyKjEjBBIEQCUAARYBEQMxMCIBACNCACRCMlEzUUAAUDM1MhAjEgAxAAISAVFQECAhQAMBMhEzEgASNxAhFRIxECMRJBQAERAToBgQMhJSRQFAEhAwMiIhMQAwAgQiBQJiIGMQQhEiQxR1MiAjIAIEEiAkARECEzQlMjECIRATBgIhEBQAIQAEATEjBCMwAgMBMhAhIyFBIxQAARI1AAEABCIDFBIRUzMBIgAgEiARQCASMQQDQCFBAQAUJwMUElAyIAIRBSIRITICEAIxMAEUBEYTcBMBEEIxMREwIRIDAGIAEgYxBAEANCAhBAI2UhIiIgIRABIEVRAwNEIQERQgEFMhFCQSIAEhQDMTEQMiAjJyEQ==";

  private final HashFunction fn = Hashing.murmur3_128();

  private ColumnSelectorFactory metricFactory;
  private VectorColumnSelectorFactory vectorFactory;

  @Before
  public void setup()
  {
    final ColumnCapabilitiesImpl columnCapabilities = ColumnCapabilitiesImpl.createDefault().setType(ColumnType.NESTED_DATA);
    metricFactory = new TestColumnSelectorFactory()
        .addCapabilities("uniques", columnCapabilities)
        .addColumnSelector("uniques", null);
    vectorFactory = new TestVectorColumnSelectorFactory().addCapabilities("uniques", columnCapabilities);
  }

  @Test
  public void testDeserializeV0()
  {
    Object v0 = AGGREGATOR_FACTORY.deserialize(V0_BASE64);
    Assert.assertEquals("deserialized value is VersionZeroHyperLogLogCollector", VersionZeroHyperLogLogCollector.class, v0.getClass());
  }

  @Test
  public void testCompare1()
  {
    HyperLogLogCollector collector1 = HyperLogLogCollector.makeLatestCollector();
    HyperLogLogCollector collector2 = HyperLogLogCollector.makeLatestCollector();
    collector1.add(fn.hashLong(0).asBytes());
    HyperUniquesAggregatorFactory factory = new HyperUniquesAggregatorFactory("foo", "bar");
    Comparator comparator = factory.getComparator();
    for (int i = 1; i < 100; i = i + 2) {
      collector1.add(fn.hashLong(i).asBytes());
      collector2.add(fn.hashLong(i + 1).asBytes());
      Assert.assertEquals(1, comparator.compare(collector1, collector2));
      Assert.assertEquals(1, Double.compare(collector1.estimateCardinality(), collector2.estimateCardinality()));
    }
  }

  @Test
  public void testCompare2()
  {
    Random rand = new Random(0);
    HyperUniquesAggregatorFactory factory = new HyperUniquesAggregatorFactory("foo", "bar");
    Comparator comparator = factory.getComparator();
    for (int i = 1; i < 1000; ++i) {
      HyperLogLogCollector collector1 = HyperLogLogCollector.makeLatestCollector();
      int j = rand.nextInt(50);
      for (int l = 0; l < j; ++l) {
        collector1.add(fn.hashLong(rand.nextLong()).asBytes());
      }

      HyperLogLogCollector collector2 = HyperLogLogCollector.makeLatestCollector();
      int k = j + 1 + rand.nextInt(5);
      for (int l = 0; l < k; ++l) {
        collector2.add(fn.hashLong(rand.nextLong()).asBytes());
      }

      Assert.assertEquals(
          Double.compare(collector1.estimateCardinality(), collector2.estimateCardinality()),
          comparator.compare(collector1, collector2)
      );
    }

    for (int i = 1; i < 100; ++i) {
      HyperLogLogCollector collector1 = HyperLogLogCollector.makeLatestCollector();
      int j = rand.nextInt(500);
      for (int l = 0; l < j; ++l) {
        collector1.add(fn.hashLong(rand.nextLong()).asBytes());
      }

      HyperLogLogCollector collector2 = HyperLogLogCollector.makeLatestCollector();
      int k = j + 2 + rand.nextInt(5);
      for (int l = 0; l < k; ++l) {
        collector2.add(fn.hashLong(rand.nextLong()).asBytes());
      }

      Assert.assertEquals(
          Double.compare(collector1.estimateCardinality(), collector2.estimateCardinality()),
          comparator.compare(collector1, collector2)
      );
    }

    for (int i = 1; i < 10; ++i) {
      HyperLogLogCollector collector1 = HyperLogLogCollector.makeLatestCollector();
      int j = rand.nextInt(100000);
      for (int l = 0; l < j; ++l) {
        collector1.add(fn.hashLong(rand.nextLong()).asBytes());
      }

      HyperLogLogCollector collector2 = HyperLogLogCollector.makeLatestCollector();
      int k = j + 20000 + rand.nextInt(100000);
      for (int l = 0; l < k; ++l) {
        collector2.add(fn.hashLong(rand.nextLong()).asBytes());
      }

      Assert.assertEquals(
          Double.compare(collector1.estimateCardinality(), collector2.estimateCardinality()),
          comparator.compare(collector1, collector2)
      );
    }
  }

  @Test
  public void testCompareToShouldBehaveConsistentlyWithEstimatedCardinalitiesEvenInToughCases()
  {
    // given
    Random rand = new Random(0);
    HyperUniquesAggregatorFactory factory = new HyperUniquesAggregatorFactory("foo", "bar");
    Comparator comparator = factory.getComparator();

    for (int i = 0; i < 1000; ++i) {
      // given
      HyperLogLogCollector leftCollector = HyperLogLogCollector.makeLatestCollector();
      int j = rand.nextInt(9000) + 5000;
      for (int l = 0; l < j; ++l) {
        leftCollector.add(fn.hashLong(rand.nextLong()).asBytes());
      }

      HyperLogLogCollector rightCollector = HyperLogLogCollector.makeLatestCollector();
      int k = rand.nextInt(9000) + 5000;
      for (int l = 0; l < k; ++l) {
        rightCollector.add(fn.hashLong(rand.nextLong()).asBytes());
      }

      // when
      final int orderedByCardinality = Double.compare(
          leftCollector.estimateCardinality(),
          rightCollector.estimateCardinality()
      );
      final int orderedByComparator = comparator.compare(leftCollector, rightCollector);

      // then, assert hyperloglog comparator behaves consistently with estimated cardinalities
      Assert.assertEquals(
          StringUtils.format("orderedByComparator=%d, orderedByCardinality=%d,\n" +
                             "Left={cardinality=%f, hll=%s},\n" +
                             "Right={cardinality=%f, hll=%s},\n", orderedByComparator, orderedByCardinality,
                             leftCollector.estimateCardinality(), leftCollector,
                             rightCollector.estimateCardinality(), rightCollector
          ),
          orderedByCardinality,
          orderedByComparator
      );
    }
  }

  @Test
  public void testEstimateCardinalityForZeroCardinality()
  {
    HyperLogLogCollector emptyHyperLogLogCollector = HyperUniquesBufferAggregator.doGet(
        ByteBuffer.allocate(HyperLogLogCollector.getLatestNumBytesForDenseStorage()),
        0
    );

    Assert.assertEquals(0L, HyperUniquesAggregatorFactory.estimateCardinality(null, true));
    Assert.assertEquals(0d, HyperUniquesAggregatorFactory.estimateCardinality(null, false));

    Assert.assertEquals(0L, HyperUniquesAggregatorFactory.estimateCardinality(emptyHyperLogLogCollector, true));
    Assert.assertEquals(0d, HyperUniquesAggregatorFactory.estimateCardinality(emptyHyperLogLogCollector, false));

    Assert.assertEquals(
        HyperUniquesAggregatorFactory.estimateCardinality(emptyHyperLogLogCollector, true).getClass(),
        HyperUniquesAggregatorFactory.estimateCardinality(null, true).getClass()
    );
    Assert.assertEquals(
        HyperUniquesAggregatorFactory.estimateCardinality(emptyHyperLogLogCollector, false).getClass(),
        HyperUniquesAggregatorFactory.estimateCardinality(null, false).getClass()
    );
  }

  @Test
  public void testSerde() throws Exception
  {
    final HyperUniquesAggregatorFactory factory = new HyperUniquesAggregatorFactory(
        "foo",
        "bar",
        true,
        true
    );

    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    final AggregatorFactory factory2 = jsonMapper.readValue(
        jsonMapper.writeValueAsString(factory),
        AggregatorFactory.class
    );

    Assert.assertEquals(factory, factory2);
  }

  @Test
  public void testFactorizeOnPrimitiveColumnType()
  {
    final ColumnCapabilitiesImpl columnCapabilities = ColumnCapabilitiesImpl.createDefault().setType(ColumnType.LONG);
    final ColumnSelectorFactory metricFactory = new TestColumnSelectorFactory()
        .addCapabilities("uniques", columnCapabilities)
        .addColumnSelector("uniques", NilColumnValueSelector.instance());
    final VectorColumnSelectorFactory vectorFactory = new TestVectorColumnSelectorFactory().addCapabilities("uniques", columnCapabilities);

    Assert.assertEquals(NoopAggregator.instance(), AGGREGATOR_FACTORY.factorize(metricFactory));
    Assert.assertEquals(NoopBufferAggregator.instance(), AGGREGATOR_FACTORY.factorizeBuffered(metricFactory));
    Assert.assertEquals(NoopVectorAggregator.instance(), AGGREGATOR_FACTORY.factorizeVector(vectorFactory));
  }

  @Test
  public void testFactorizeOnUnsupportedComplexColumn()
  {
    Throwable exception = assertThrows(DruidException.class, () -> AGGREGATOR_FACTORY.factorize(metricFactory));
    Assert.assertEquals("Using aggregator [hyperUnique] is not supported for complex columns with type [COMPLEX<json>].", exception.getMessage());
  }

  @Test
  public void testFactorizeBufferedOnUnsupportedComplexColumn()
  {
    Throwable exception = assertThrows(DruidException.class, () -> AGGREGATOR_FACTORY.factorizeBuffered(metricFactory));
    Assert.assertEquals("Using aggregator [hyperUnique] is not supported for complex columns with type [COMPLEX<json>].", exception.getMessage());
  }

  @Test
  public void testFactorizeVectorOnUnsupportedComplexColumn()
  {
    Throwable exception = assertThrows(DruidException.class, () -> AGGREGATOR_FACTORY.factorizeVector(vectorFactory));
    Assert.assertEquals("Using aggregator [hyperUnique] is not supported for complex columns with type [COMPLEX<json>].", exception.getMessage());
  }
}
