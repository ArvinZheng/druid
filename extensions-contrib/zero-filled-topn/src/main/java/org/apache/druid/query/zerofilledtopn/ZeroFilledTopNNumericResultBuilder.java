package org.apache.druid.query.zerofilledtopn;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.topn.DimValHolder;
import org.apache.druid.query.topn.DimensionAndMetricValueExtractor;
import org.apache.druid.query.topn.TopNResultBuilder;
import org.apache.druid.query.topn.TopNResultValue;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;

public class ZeroFilledTopNNumericResultBuilder implements TopNResultBuilder {

    private final DateTime timestamp;
    private final DimensionSpec dimSpec;
    private final String metricName;
    private final List<PostAggregator> postAggs;
    private final PriorityQueue<DimValHolder> pQueue;
    private final String[] aggFactoryNames;
    private static final Comparator<Comparable> dimValueComparator = new Comparator<Comparable>() {
        @Override
        public int compare(Comparable o1, Comparable o2) {
            int retval;
            if (null == o1) {
                if (null == o2) {
                    retval = 0;
                } else {
                    retval = -1;
                }
            } else if (null == o2) {
                retval = 1;
            } else {
                //noinspection unchecked
                retval = o1.compareTo(o2);
            }
            return retval;
        }
    };
    private final int threshold;
    private final Comparator metricComparator;
    private final Set<Comparable> dimValues;
    private final boolean inverted;
    private final List<DimValHolder> segmentRecords;
    private final List<AggregatorFactory> aggregatorFactories;

    public ZeroFilledTopNNumericResultBuilder(
            DateTime timestamp,
            DimensionSpec dimSpec,
            String metricName,
            int threshold,
            final Comparator comparator,
            List<AggregatorFactory> aggFactories,
            List<PostAggregator> postAggs,
            Set<Comparable> dimValues,
            boolean inverted
    ) {
        this.timestamp = timestamp;
        this.dimSpec = dimSpec;
        this.metricName = metricName;
        this.aggregatorFactories = aggFactories;
        this.aggFactoryNames = ZeroFilledTopNQueryQueryToolChest.extractFactoryName(aggFactories);

        this.postAggs = AggregatorUtil.pruneDependentPostAgg(postAggs, this.metricName);
        this.threshold = threshold;
        this.metricComparator = comparator;

        final Comparator<DimValHolder> dimValHolderComparator = (d1, d2) -> {
            //noinspection unchecked
            int retVal = metricComparator.compare(d1.getTopNMetricVal(), d2.getTopNMetricVal());

            if (retVal == 0) {
                retVal = dimValueComparator.compare(d1.getDimValue(), d2.getDimValue());
            }

            return retVal;
        };

        // The logic in addEntry first adds, then removes if needed. So it can at any point have up to threshold + 1 entries.
        pQueue = new PriorityQueue<>(this.threshold + 1, dimValHolderComparator);
        this.dimValues = Sets.newTreeSet(dimValueComparator);
        this.dimValues.addAll(dimValues);
        this.inverted = inverted;
        this.segmentRecords = Lists.newArrayListWithCapacity(dimValues.size());
    }

    private static final int LOOP_UNROLL_COUNT = 8;

    @Override
    public ZeroFilledTopNNumericResultBuilder addEntry(
            Comparable dimValueObj,
            Object dimValIndex,
            Object[] metricVals
    ) {
        Preconditions.checkArgument(
                metricVals.length == aggFactoryNames.length,
                "metricVals must be the same length as aggFactories"
        );

        final Map<String, Object> metricValues = getMetricValues(dimValueObj, metricVals);

        Object topNMetricVal = metricValues.get(metricName);

        if (shouldAdd(topNMetricVal)) {
            DimValHolder dimValHolder = new DimValHolder.Builder()
                    .withTopNMetricVal(topNMetricVal)
                    .withDimValue(dimValueObj)
                    .withDimValIndex(dimValIndex)
                    .withMetricValues(metricValues)
                    .build();
            pQueue.add(dimValHolder);
            segmentRecords.add(dimValHolder);
        }
        if (this.pQueue.size() > this.threshold) {
            pQueue.poll();
        }

        return this;
    }

    private Map<String, Object> getMetricValues(Comparable dimValueObj, Object[] metricVals) {
        final Map<String, Object> metricValues = Maps.newHashMapWithExpectedSize(metricVals.length + postAggs.size() + 1);

        metricValues.put(dimSpec.getOutputName(), dimValueObj);

        final int extra = metricVals.length % LOOP_UNROLL_COUNT;

        switch (extra) {
            case 7:
                metricValues.put(aggFactoryNames[6], metricVals[6]);
                // fall through
            case 6:
                metricValues.put(aggFactoryNames[5], metricVals[5]);
                // fall through
            case 5:
                metricValues.put(aggFactoryNames[4], metricVals[4]);
                // fall through
            case 4:
                metricValues.put(aggFactoryNames[3], metricVals[3]);
                // fall through
            case 3:
                metricValues.put(aggFactoryNames[2], metricVals[2]);
                // fall through
            case 2:
                metricValues.put(aggFactoryNames[1], metricVals[1]);
                // fall through
            case 1:
                metricValues.put(aggFactoryNames[0], metricVals[0]);
        }
        for (int i = extra; i < metricVals.length; i += LOOP_UNROLL_COUNT) {
            metricValues.put(aggFactoryNames[i + 0], metricVals[i + 0]);
            // LGTM.com flags this, but it's safe
            // because we know "metricVals.length - extra" is a multiple of LOOP_UNROLL_COUNT.
            metricValues.put(aggFactoryNames[i + 1], metricVals[i + 1]); // lgtm [java/index-out-of-bounds]
            metricValues.put(aggFactoryNames[i + 2], metricVals[i + 2]); // lgtm [java/index-out-of-bounds]
            metricValues.put(aggFactoryNames[i + 3], metricVals[i + 3]); // lgtm [java/index-out-of-bounds]
            metricValues.put(aggFactoryNames[i + 4], metricVals[i + 4]); // lgtm [java/index-out-of-bounds]
            metricValues.put(aggFactoryNames[i + 5], metricVals[i + 5]); // lgtm [java/index-out-of-bounds]
            metricValues.put(aggFactoryNames[i + 6], metricVals[i + 6]); // lgtm [java/index-out-of-bounds]
            metricValues.put(aggFactoryNames[i + 7], metricVals[i + 7]); // lgtm [java/index-out-of-bounds]
        }

        // Order matters here, do not unroll
        for (PostAggregator postAgg : postAggs) {
            metricValues.put(postAgg.getName(), postAgg.compute(metricValues));
        }
        return metricValues;
    }

    private boolean shouldAdd(Object topNMetricVal) {
        final boolean belowThreshold = pQueue.size() < this.threshold;
        final boolean belowMax = belowThreshold
                || this.metricComparator.compare(pQueue.peek().getTopNMetricVal(), topNMetricVal) < 0;
        return belowMax;
    }

    @Override
    public TopNResultBuilder addEntry(DimensionAndMetricValueExtractor dimensionAndMetricValueExtractor) {
        final Object dimValue = dimensionAndMetricValueExtractor.getDimensionValue(metricName);

        if (shouldAdd(dimValue)) {
            final DimValHolder valHolder = new DimValHolder.Builder()
                    .withTopNMetricVal(dimValue)
                    .withDimValue((Comparable) dimensionAndMetricValueExtractor.getDimensionValue(dimSpec.getOutputName()))
                    .withMetricValues(dimensionAndMetricValueExtractor.getBaseObject())
                    .build();
            pQueue.add(valHolder);
            segmentRecords.add(valHolder);
        }
        if (pQueue.size() > this.threshold) {
            pQueue.poll(); // throw away
        }
        return this;
    }

    private void zeroFillIfNeeded() {
        // ascending
        if (inverted) {
            Set<Comparable> segmentDimValues = segmentRecords.stream().map(DimValHolder::getDimValue).collect(Collectors.toSet());
            // dimensions that are not existing in segment, sort them by dimension comparator
            List<Comparable> zeroFillCandidates = Lists.newArrayList(Sets.difference(dimValues, segmentDimValues));
            if (!zeroFillCandidates.isEmpty()) {
                zeroFill(zeroFillCandidates, threshold);
            }
        }
        // descending
        else {
            if (pQueue.size() < threshold) {
                int numberToBeZeroFilled = threshold - pQueue.size();
                // remove all dimension values that are seen in segment
                List<Comparable> zeroFillCandidates = Lists.newArrayList(dimValues);
                zeroFillCandidates.removeAll(segmentRecords.stream().map(DimValHolder::getDimValue).collect(Collectors.toList()));
                zeroFill(zeroFillCandidates, numberToBeZeroFilled);
            }
        }
    }

    private void zeroFill(List<Comparable> zeroFillCandidates, int numberToBeZeroFilled) {
        Collections.sort(zeroFillCandidates, dimValueComparator);
        int min = Math.min(zeroFillCandidates.size(), numberToBeZeroFilled);
        for (int i = 0; i < min; i++) {
            pQueue.add(createZeroEntry(zeroFillCandidates.get(i)));
            if (this.pQueue.size() > this.threshold) {
                pQueue.poll();
            }
        }
    }

    private DimValHolder createZeroEntry(Comparable dimValueObj) {
        Map<String, Object> retVal = new LinkedHashMap<>(aggFactoryNames.length + postAggs.size() + 1);

        retVal.put(dimSpec.getDimension(), dimValueObj);
        for (AggregatorFactory factory : aggregatorFactories) {
            retVal.put(factory.getName(), getZeroByType(factory));
        }

        for (PostAggregator pf : postAggs) {
            // TODO: what is the real data type can be?
            retVal.put(pf.getName(), 0d);
        }

        DimensionAndMetricValueExtractor dimensionAndMetricValueExtractor = new DimensionAndMetricValueExtractor(retVal);

        return new DimValHolder.Builder()
                .withTopNMetricVal(dimensionAndMetricValueExtractor.getDimensionValue(metricName))
                .withDimValue(dimValueObj)
                .withMetricValues(dimensionAndMetricValueExtractor.getBaseObject())
                .build();
    }

    private Object getZeroByType(AggregatorFactory factory) {
        Object ret = 0L;
        switch (factory.getTypeName()) {
            case "double":
                ret = new Double(0);
                break;
            case "float":
                ret = new Float(0);
                break;
            case "long":
            default:
                ret = new Long(0);
        }
        return ret;
    }

    @Override
    public Iterator<DimValHolder> getTopNIterator() {
        return pQueue.iterator();
    }

    @Override
    public Result<TopNResultValue> build() {

        zeroFillIfNeeded();

        final DimValHolder[] holderValueArray = pQueue.toArray(new DimValHolder[0]);
        Arrays.sort(
                holderValueArray,
                (d1, d2) -> {
                    // Metric values flipped compared to dimValueHolderComparator.

                    //noinspection unchecked
                    int retVal = metricComparator.compare(d2.getTopNMetricVal(), d1.getTopNMetricVal());

                    if (retVal == 0) {
                        retVal = dimValueComparator.compare(d1.getDimValue(), d2.getDimValue());
                    }

                    return retVal;
                }
        );
        List<DimValHolder> holderValues = Arrays.asList(holderValueArray);

        // Pull out top aggregated values
        final List<Map<String, Object>> values = Lists.transform(holderValues, DimValHolder::getMetricValues);
        return new Result<>(timestamp, new ZeroFilledTopNResultValue(values));
    }

}
