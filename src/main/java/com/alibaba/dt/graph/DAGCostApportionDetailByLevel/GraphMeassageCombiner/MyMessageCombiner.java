package com.alibaba.dt.graph.DAGCostApportionDetailByLevel.GraphMeassageCombiner;

import com.alibaba.dt.graph.DAGCostApportionDetailByLevel.GraphJobDomain;
import com.aliyun.odps.graph.Combiner;
import com.aliyun.odps.io.DoubleWritable;
import com.aliyun.odps.io.MapWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.io.Writable;

import java.io.IOException;

public class MyMessageCombiner extends Combiner<Text, MapWritable> {
    private static final GraphJobDomain gd = new GraphJobDomain();

    /**
     * 对于发送给同一个vertexId的多个消息进行聚合从而节省空间
     * combine操作会在两个combinedMessage和 MessageToCombined之间进行
     *
     * @param vertexId
     * @param combinedMessage
     * @param messageToCombine
     * @throws IOException
     */
    @Override
    public void combine(Text vertexId, MapWritable combinedMessage, MapWritable messageToCombine) throws IOException {

        if(!combinedMessage.containsKey(gd.MSG_NODE_LIST)){
            combinedMessage.put(gd.MSG_NODE_LIST, new MapWritable());
        }
        if(!combinedMessage.containsKey(gd.NODE_ANCESTOR_APPORTION_MAP)){
            combinedMessage.put(gd.NODE_ANCESTOR_APPORTION_MAP, new MapWritable());
        }

        if(messageToCombine.containsKey(gd.MSG_NODE_LIST)){
            MapWritable toCombinedNodeList = (MapWritable) messageToCombine.get(gd.MSG_NODE_LIST);
            MapWritable combinedNodeList = (MapWritable) combinedMessage.get(gd.MSG_NODE_LIST);
            if(toCombinedNodeList.size() > 0) {
                for (Writable toCombinedNodeId : toCombinedNodeList.keySet()) {
                    combinedNodeList.put(toCombinedNodeId, gd.TEXT_EMPTY);
                }
                combinedMessage.put(gd.MSG_NODE_LIST, combinedNodeList);
            }
        }

        if(messageToCombine.containsKey(gd.NODE_ANCESTOR_APPORTION_MAP)){
            MapWritable toCombinedCostMap = (MapWritable) messageToCombine.get(gd.NODE_ANCESTOR_APPORTION_MAP);
            MapWritable combinedCostMap = (MapWritable) combinedMessage.get(gd.NODE_ANCESTOR_APPORTION_MAP);
            if(toCombinedCostMap.size() > 0){
                for(Writable toCombinedNodeId : toCombinedCostMap.keySet()){
                    if(combinedCostMap.containsKey(toCombinedNodeId)){
                        double toCombinedCost = ((DoubleWritable)toCombinedCostMap.get(toCombinedNodeId)).get();
                        double combinedCost = ((DoubleWritable)combinedCostMap.get(toCombinedNodeId)).get();
                        combinedCostMap.put(toCombinedNodeId, new DoubleWritable(toCombinedCost+ combinedCost));
                    }else {
                        combinedCostMap.put(toCombinedNodeId, toCombinedCostMap.get(toCombinedNodeId));
                    }
                }
                combinedMessage.put(gd.NODE_ANCESTOR_APPORTION_MAP, combinedCostMap);
            }
        }
    }
}
