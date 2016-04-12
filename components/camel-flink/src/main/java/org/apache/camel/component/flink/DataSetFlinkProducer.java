package org.apache.camel.component.flink;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultProducer;
import org.apache.flink.api.java.DataSet;

import java.util.List;

public class DataSetFlinkProducer extends DefaultProducer {

    public DataSetFlinkProducer(FlinkEndpoint endpoint) {
        super(endpoint);
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        DataSet ds = resolveDataSet(exchange);
        DataSetCallback dataSetCallback = resolveDataSetCallback(exchange);
        Object body = exchange.getIn().getBody();
        Object result = body instanceof List ? dataSetCallback.onDataSet(ds, ((List) body).toArray(new Object[0])) : dataSetCallback.onDataSet(ds, body);
        collectResults(exchange, result);
    }

    @Override
    public FlinkEndpoint getEndpoint() {
        return (FlinkEndpoint) super.getEndpoint();
    }

    protected void collectResults(Exchange exchange, Object result) throws Exception {
        if (result instanceof DataSet) {
            DataSet dsResults = (DataSet) result;
            if (getEndpoint().isCollect()) {
                exchange.getIn().setBody(dsResults.collect());
            }
            else {
                exchange.getIn().setBody(result);
                exchange.getIn().setHeader(FlinkConstants.FLINK_DATASET_HEADER, result);
            }
        }
        else {
            exchange.getIn().setBody(result);
        }
    }

    protected DataSet resolveDataSet(Exchange exchange) {
        if (exchange.getIn().getHeader(FlinkConstants.FLINK_DATASET_HEADER) != null) {
            return (DataSet) exchange.getIn().getHeader(FlinkConstants.FLINK_DATASET_HEADER);
        } else if (getEndpoint().getDataSet() != null) {
            return getEndpoint().getDataSet();
        } else {
            throw new IllegalStateException("No DataSet defined");
        }
    }

    protected DataSetCallback resolveDataSetCallback(Exchange exchange) {
        if (exchange.getIn().getHeader(FlinkConstants.FLINK_DATASET_CALLBACK_HEADER) != null) {
            return (DataSetCallback) exchange.getIn().getHeader(FlinkConstants.FLINK_DATASET_CALLBACK_HEADER);
        } else if (getEndpoint().getDataSetCallback() != null) {
            return getEndpoint().getDataSetCallback();
        } else {
            throw new IllegalStateException("Cannot resolve DataSet callback.");
        }
    }
}