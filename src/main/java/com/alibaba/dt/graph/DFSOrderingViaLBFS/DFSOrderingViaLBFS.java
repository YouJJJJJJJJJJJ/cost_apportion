package com.alibaba.dt.graph.DFSOrderingViaLBFS;

import com.alibaba.dt.graph.DFSOrderingViaLBFS.GraphAggregator.MyGraphAggregator;
import com.alibaba.dt.graph.DFSOrderingViaLBFS.GraphReader.MyGraphReader;
import com.alibaba.dt.graph.DFSOrderingViaLBFS.GraphVertex.MyGraphVertex;
import com.alibaba.dt.graph.DFSOrderingViaLBFS.GraphVertexLoadingResolver.MyVertexLoadingResolver;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.graph.GraphJob;

public class DFSOrderingViaLBFS {
    private static final GraphJobDomain gd = new GraphJobDomain();

    private static int getMaxSuperStep(String[] args){
        if(args.length >= 4){
            int maxStep = Integer.parseInt(args[3]);
            if(maxStep > 0){
                return maxStep;
            }else {
                return -1;
            }
        }else {
            return 32;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.out.println("Please make sure your inputs parameters are in the form of <startVertexTable, AllVertexTable, edgeTable, outTable>");
            System.exit(-1);
        }

        GraphJob job = new GraphJob();
        job.setGraphLoaderClass(MyGraphReader.class);
        job.setVertexClass(MyGraphVertex.class);
        //job.setCombinerClass(MyMessageCombiner.class);
        job.setLoadingVertexResolver(MyVertexLoadingResolver.class);
        job.setAggregatorClass(MyGraphAggregator.class);
        //start vertex
        //job.addInput(TableInfo.builder().tableName(args[0]).build());
        //universal vertices
        job.addInput(TableInfo.builder().tableName(args[0]).build());
        //edge
        job.addInput(TableInfo.builder().tableName(args[1]).build());
        //output table
        job.addOutput(TableInfo.builder().tableName(args[2]).build());

        //设置edge表的名字到全局变量
        //job.set(gd.SRC_LABEL_TABLE, args[0]);
        job.set(gd.EDGE_TABLE, args[1]);
        //设置超步上线
        job.setMaxIteration(getMaxSuperStep(args));
        //long startTime = System.currentTimeMillis();
        job.run();
        //long endTime = System.currentTimeMillis();
        //double totalTime = endTime - startTime;
        //System.out.print("=================Total Time:" + totalTime + "ms, " + totalTime/1000.0 + "s ==============" + "\n");
    }
}
