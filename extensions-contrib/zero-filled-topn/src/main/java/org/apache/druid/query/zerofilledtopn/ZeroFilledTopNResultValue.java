package org.apache.druid.query.zerofilledtopn;

import org.apache.druid.query.topn.TopNResultValue;

import java.util.List;

public class ZeroFilledTopNResultValue extends TopNResultValue {

    public ZeroFilledTopNResultValue(List<?> values) {
        super(values);
    }

}
