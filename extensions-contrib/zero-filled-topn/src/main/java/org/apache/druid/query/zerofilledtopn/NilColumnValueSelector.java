package org.apache.druid.query.zerofilledtopn;

import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;

public enum NilColumnValueSelector implements ColumnValueSelector {

    INSTANCE;

    @Override
    public double getDouble() {
        return 0.0d;
    }

    @Override
    public float getFloat() {
        return 0.0f;
    }

    @Override
    public long getLong() {
        return 0L;
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector) {
        // do nothing
    }

    @Nullable
    @Override
    public Object getObject() {
        return null;
    }

    @Override
    public Class classOfObject() {
        return Object.class;
    }

    @Override
    public boolean isNull() {
        return true;
    }

}
