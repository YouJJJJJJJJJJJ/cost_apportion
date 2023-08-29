package com.alibaba.dt.graph.DAGCostApportionDetailByLevel.GraphAggregator;

import com.aliyun.odps.graph.Aggregator;
import com.aliyun.odps.graph.WorkerContext;
import com.aliyun.odps.io.Text;

import java.io.IOException;
public class MyGraphAggregator extends Aggregator<Text> {

    @Override
    public Text createInitialValue(WorkerContext context) throws IOException {
        return new Text("");
    }

    @Override
    public void aggregate(Text value, Object item) throws IOException{
        value.set("");
    }

    @Override
    public void merge(Text mainValue, Text partialValue) {
        mainValue.set("");
    }

    @Override
    public boolean terminate(WorkerContext context, Text value) throws IOException {

        if(context.getSuperstep() == context.getMaxIteration()){
            return true;
        }else {
            return false;
        }
    }
}
