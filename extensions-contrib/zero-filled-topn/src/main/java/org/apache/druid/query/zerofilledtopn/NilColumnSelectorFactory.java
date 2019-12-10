package org.apache.druid.query.zerofilledtopn;

import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnCapabilities;

import javax.annotation.Nullable;

public enum NilColumnSelectorFactory implements ColumnSelectorFactory {

    INSTANCE;

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec) {
        return DimensionSelector.constant(null);
    }

    @Override
    public ColumnValueSelector makeColumnValueSelector(String columnName) {
        return NilColumnValueSelector.INSTANCE;
    }

    @Nullable
    @Override
    public ColumnCapabilities getColumnCapabilities(String column) {
        return null;
    }

}
