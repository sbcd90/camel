package org.apache.camel.component.flink;

import org.apache.camel.Endpoint;
import org.apache.camel.impl.UriEndpointComponent;
import org.apache.flink.api.java.DataSet;

import java.util.Map;

/**
 * The flink component can be used to send DataSet or DataStream jobs to Apache Flink cluster.
 */
public class FlinkComponent extends UriEndpointComponent {

    private DataSet ds;
    private DataSetCallback dataSetCallback;

    public FlinkComponent() {
        super(FlinkEndpoint.class);
    }

    @Override
    protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
        EndpointType type = getCamelContext().getTypeConverter().mandatoryConvertTo(EndpointType.class, remaining);
        return new FlinkEndpoint(uri, this, type);
    }

    public DataSet getDataSet() {
        return ds;
    }

    /**
     * DataSet to compute against.
     */
    public void setDataSet(DataSet ds) {
        this.ds = ds;
    }

    public DataSetCallback getDataSetCallback() {
        return dataSetCallback;
    }

    /**
     * Function performing action against a DataSet.
     */
    public void setDataSetCallback(DataSetCallback dataSetCallback) {
        this.dataSetCallback = dataSetCallback;
    }
}