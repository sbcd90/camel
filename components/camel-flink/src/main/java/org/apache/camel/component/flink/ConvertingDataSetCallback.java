package org.apache.camel.component.flink;

import org.apache.camel.CamelContext;
import org.apache.flink.api.java.DataSet;

import static java.lang.String.format;

public abstract class ConvertingDataSetCallback<T> implements DataSetCallback<T> {

    private final CamelContext camelContext;

    private final Class[] payloadTypes;

    public ConvertingDataSetCallback(CamelContext camelContext, Class... payloadTypes) {
        this.camelContext = camelContext;
        this.payloadTypes = payloadTypes;
    }

    @Override
    public T onDataSet(DataSet ds, Object... payloads) {
        if (payloads.length != payloadTypes.length) {
            String message = format("Received %d payloads, but expected %d.", payloads.length, payloadTypes.length);
            throw new IllegalArgumentException(message);
        }
        for (int i=0; i < payloads.length;i++) {
            payloads[i] = camelContext.getTypeConverter().convertTo(payloadTypes[i], payloads[i]);
        }
        return doOnDataSet(ds, payloads);
    }

    public abstract T doOnDataSet(DataSet ds, Object... payloads);
}