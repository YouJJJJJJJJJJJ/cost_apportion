package com.alibaba.dt.graph.DAGCostApportionDetailByLevel.GraphVertexLoadingResolver;

import com.alibaba.dt.graph.DAGCostApportionDetailByLevel.GraphJobDomain;
import com.alibaba.dt.graph.DAGCostApportionDetailByLevel.GraphVertex.MyGraphVertex;
import com.aliyun.odps.graph.Edge;
import com.aliyun.odps.graph.LoadingVertexResolver;
import com.aliyun.odps.graph.Vertex;
import com.aliyun.odps.graph.VertexChanges;
import com.aliyun.odps.io.MapWritable;
import com.aliyun.odps.io.Text;

import java.io.IOException;
import java.util.List;

public class MyVertexLoadingResolver extends LoadingVertexResolver<Text, MapWritable, MapWritable, MapWritable> {
    private static final GraphJobDomain gd = new GraphJobDomain();

    public static boolean ifStartVertex(MapWritable vertexValue){
        return Boolean.parseBoolean(vertexValue.get(gd.IF_START).toString());
    }
    /**
     * 该类会在graph reader后执行一次，对需要add/delete的定点进行同一操作；
     * 拥有相同vertex id都会在vertexChanges中出现
     */
    private static MyGraphVertex mergeVertexChangeList(List<Vertex<Text, MapWritable, MapWritable, MapWritable>> vertexChangesList){
        MyGraphVertex finalVertex = null;
        for(Vertex vertex: vertexChangesList){
            MyGraphVertex myGraphVertex = (MyGraphVertex)vertex;
            if(finalVertex == null) {
                finalVertex = myGraphVertex;
            }//将start为true的vertex返回
            else if( ifStartVertex(myGraphVertex.getValue()) ) {
                finalVertex = myGraphVertex;
            }
        }
        return finalVertex;
    }


    @Override
    public Vertex<Text, MapWritable, MapWritable, MapWritable> resolve(Text vertexId,
        VertexChanges<Text, MapWritable, MapWritable, MapWritable> vertexChanges) throws IOException {
        //初始化一个Vertex类
        MyGraphVertex computeVertex = null;
        //如果现有的vertexChanges中没有任何的vertex
        if (vertexChanges.getAddedVertexList() == null || vertexChanges.getAddedVertexList().isEmpty()) {
            computeVertex = new MyGraphVertex();
            computeVertex.setId(vertexId);
            computeVertex.setValue(new MapWritable());
        }//如果存在一个或者多个，则取第一个；
        else {
            computeVertex = (MyGraphVertex) vertexChanges.getAddedVertexList().get(0);
        }
        //将所有指向vertexId的边全部汇总到一个vertex实例上；
        if (vertexChanges.getAddedEdgeList() != null) {
            for (Edge<Text, MapWritable> edge : vertexChanges.getAddedEdgeList()) {
                computeVertex.addEdge(edge.getDestVertexId(), edge.getValue());
            }
        }
        return computeVertex;
    }
}
