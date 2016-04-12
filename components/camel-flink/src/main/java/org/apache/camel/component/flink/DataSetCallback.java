package org.apache.camel.component.flink;

import org.apache.flink.api.java.DataSet;

public interface DataSetCallback<T> {

    T onDataSet(DataSet ds, Object... payloads);
}