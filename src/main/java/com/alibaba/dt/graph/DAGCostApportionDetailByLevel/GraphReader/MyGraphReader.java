package com.alibaba.dt.graph.DAGCostApportionDetailByLevel.GraphReader;

import com.alibaba.dt.graph.DAGCostApportionDetailByLevel.GraphJobDomain;
import com.alibaba.dt.graph.DAGCostApportionDetailByLevel.GraphVertex.MyGraphVertex;
import com.aliyun.odps.conf.Configuration;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.graph.Edge;
import com.aliyun.odps.graph.GraphLoader;
import com.aliyun.odps.graph.MutationContext;
import com.aliyun.odps.io.*;

import java.io.IOException;

public class MyGraphReader extends GraphLoader<Text, MapWritable, MapWritable, MapWritable> {
    private static final GraphJobDomain gd = new GraphJobDomain();
    boolean isEdgeData;

    private static MapWritable setVertexStartMark(boolean ifStart, WritableRecord record){
        MapWritable vertexRawValue = new MapWritable();
        MapWritable nodeCost = new MapWritable();
        nodeCost.put(record.get(0), new DoubleWritable( Double.parseDouble(record.get(1).toString())) );
        vertexRawValue.put(gd.NODE_COST, nodeCost);
        vertexRawValue.put(gd.IF_LEAF, new BooleanWritable( Boolean.parseBoolean(record.get(2).toString()) ) );
        vertexRawValue.put(gd.IF_START, new BooleanWritable(ifStart));
        return vertexRawValue;
    }

    private static MapWritable setEdgeValue(WritableRecord record){
        MapWritable edgeValue = new MapWritable();
        edgeValue.put(gd.EDGE_WEIGHT, record.get(2));
        return edgeValue;
    }

    @Override
    public void setup(Configuration conf, int workerId, TableInfo inputTableInfo) {
        if (!(inputTableInfo == null)) {
            isEdgeData = conf.get(gd.EDGE_TABLE).equals(inputTableInfo.getTableName());
        }
    }

    @Override
    public void load(LongWritable recordNum,
        WritableRecord record,
        MutationContext<Text, MapWritable, MapWritable, MapWritable> context)
        throws IOException {
        if (isEdgeData) {
            Text sourceVertexId = (Text)record.get(0);
            Text destinationVertexId = (Text)record.get(1);
            Edge<Text, MapWritable> edge = new Edge<>(destinationVertexId, setEdgeValue(record));
            context.addEdgeRequest(sourceVertexId, edge);
        } else {
            Text vertexId = (Text)record.get(0);
            MyGraphVertex vertex = new MyGraphVertex();
            vertex.setId(vertexId);
            vertex.setValue(setVertexStartMark(false, record));
            context.addVertexRequest(vertex);
        }
    }
}
