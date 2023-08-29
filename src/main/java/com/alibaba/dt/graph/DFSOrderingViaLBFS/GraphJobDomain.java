package com.alibaba.dt.graph.DFSOrderingViaLBFS;

import com.aliyun.odps.io.BooleanWritable;
import com.aliyun.odps.io.Text;

public class GraphJobDomain {
    public final String EDGE_TABLE = "edge.table";
    public final String SRC_LABEL_TABLE = "src.label.table";
    public final String STR_DOUBLE = "double";
    public final String STRING_NUMBER_ONE = "1";
    public final String STRING_WHERE = "where";
    public final String STRING_SELECT = "select";
    public final String STRING_EMPTY = "";
    public final String STRING_TABLE = "table";
    public final String STRING_CPU = "cpu";
    public final String STRING_APPORTION = "apportion";

    public final double MINIMUM_TABLE_SIZE = 0.0001;
    public final double MINIMUM_CPU_COST = 0.00001;
    public final double MINIMUM_APPORTION_COST = 0.0001;

    public final Text TEXT_SELECT = new Text("select");
    public final Text TEXT_WHERE = new Text("where");
    public final Text TEXT_EXPR = new Text("expr");
    public final Text TEXT_DIRECT = new Text("direct");
    public final Text TEXT_GROUP = new Text("group");
    public final Text TEXT_NUMBER_ONE = new Text("1");
    public final Text TEXT_NUMBER_TWO = new Text("2");
    public final Text TEXT_NUMBER_THREE_ZERO = new Text("30");
    public final Text TEXT_NUMBER_FOUR_ZERO = new Text("40");
    public final Text TEXT_NUMBER_ZERO = new Text("0");
    public final Text TEXT_EMPTY = new Text("");
    public final Text TEXT_NULL = new Text("null");
    public final Text TEXT_TEST = new Text("test.msg");

    public final Text NODE_CATEGORY = new Text("node.category");
    public final Text NODE_VISITED_VERTEX = new Text("node.visited.vertex");
    public final Text NODE_GUID = new Text("node.guid");
    public final Text NODE_PROJECT = new Text("node.project");
    public final Text NODE_BU = new Text("node.bu");
    public final Text NODE_BG = new Text("node.bg");
    public final Text NODE_CORP = new Text("node.corp");
    public final Text NODE_WHERE_COLUMNS = new Text("node.where.col");
    public final Text NODE_APPORTION_COST = new Text("node.apportion.cost");
    public final Text NODE_CPU_COST = new Text("node.cpu.cost");
    public final Text NODE_TABLE_SIZE = new Text("node.table.size");
    public final Text NODE_PROPAGATION_TYPE = new Text("node.propagation.type");
    public final Text NODE_APPORTION_RATION = new Text("node.apportion.ration");

    public final Text EDGE_PROCESS_TYPE = new Text("process.type");
    public final Text EDGE_USE_LOCATION = new Text("edge.use.location");
    public final Text EDGE_FUNCTION_SET = new Text("edge.function.set");

    public final Text MSG_SRC_NODE_ID = new Text("src.node.id");
    public final Text MSG_SRC_NODE_PROPAGATION_TYPE = new Text("src.node.propagation.type");
    public final Text MSG_SRC_NODE_GUID = new Text("src.node.guid");
    public final Text MSG_SRC_NODE_PROJECT = new Text("src.node.project");
    public final Text MSG_SRC_NODE_BU = new Text("src.node.bu");
    public final Text MSG_SRC_NODE_BG = new Text("src.node.bg");
    public final Text MSG_SRC_NODE_CORP = new Text("src.node.corp");
    public final Text MSG_SRC_NODE_DISTANCE = new Text("src.node.distance");
    public final Text MSG_SRC_NODE_FILTERS = new Text("src.node.filters");

    public final Text MSG_NODE_LOCATION = new Text("node.location");
    public final Text MSG_NODE_PROCESS_TYPE = new Text("node.process.type");
    public final Text MSG_NODE_FUNCTIONS = new Text("node.functions");
    public final Text MSG_APPORTION_COST = new Text("node.apportion.cost");
    public final Text MSG_CPU_COST = new Text("msg.cpu.cost");
    public final Text MSG_TABLE_SIZE = new Text("msg.table.size");
    public final Text MSG_PROPAGATION_TYPE = new Text("msg.propagation.type");


    public final Text IF_CATEGORY_FIXED_NODE = new Text("fixed.category.node?");
    public final Text IF_START = new Text("if.start?");
    public final Text IF_LEAF = new Text("if.leaf?");

    public final Text UNKNOWN_LABEL = new Text("未知");
    public final Text MONEY_LABEL = new Text("金额");
    public final Text TIME_LABEL = new Text("时间");
    public final Text STATUS_LABEL = new Text("状态");
    public final Text NODE_LABEL = new Text("node.label");

    public final BooleanWritable FALSE_VALUE = new BooleanWritable(false);
    public final BooleanWritable TRUE_VALUE = new BooleanWritable(true);
}
