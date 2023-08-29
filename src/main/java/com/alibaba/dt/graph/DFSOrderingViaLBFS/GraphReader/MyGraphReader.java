package com.alibaba.dt.graph.DFSOrderingViaLBFS.GraphReader;

import com.alibaba.dt.graph.DFSOrderingViaLBFS.GraphJobDomain;
import com.alibaba.dt.graph.DFSOrderingViaLBFS.GraphVertex.MyGraphVertex;
import com.aliyun.odps.conf.Configuration;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.graph.Edge;
import com.aliyun.odps.graph.GraphLoader;
import com.aliyun.odps.graph.MutationContext;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.MapWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.io.WritableRecord;

import java.io.IOException;

public class MyGraphReader extends GraphLoader<Text, MapWritable, MapWritable, MapWritable> {
    private static final GraphJobDomain gd = new GraphJobDomain();
    boolean isEdgeData;

    private static MapWritable setVertexStartMark(boolean ifStart, WritableRecord record){
        MapWritable vertexRawValue = new MapWritable();
        Text vertexLabel = gd.TEXT_NULL;
        vertexRawValue.put(gd.NODE_LABEL, vertexLabel);
        return vertexRawValue;
    }

    private static MapWritable setEdgeValue(WritableRecord record){
        MapWritable edgeValue = new MapWritable();
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
