package org.apache.camel.component.flink;

import org.apache.flink.api.java.DataSet;

public abstract class VoidDataSetCallback implements DataSetCallback<Void> {

    public abstract void doOnDataSet(DataSet ds, Object... payloads);

    @Override
    public Void onDataSet(DataSet ds, Object... payloads) {
        doOnDataSet(ds, payloads);
        return null;
    }
}