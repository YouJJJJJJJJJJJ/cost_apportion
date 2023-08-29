# cost_apportion

## the code runs on the MaxCompute(known as ODPS).
You have two ways to repeat the experiment:

1) create four nodes on a project of MaxCompute, where the SQLs are given in /src/main/java/com/alibaba/dt/odpsSQL.
   The scheduling dependencies of the four nodes are <to_break_cycle> -> <get_node_apportion_ration> -> <get_biz_node_shared_ancestor_ratio> -> <cost_apportion_final>.
   The input of <to_break_cycle> is a table with 3 columns, named as "src_guid", "dst_guid", "ratio".
   Also you have to package four Jar-packages, two of them are for ODPS_GRAPH process and the other two are used as UDF, the codes are given in /src/main/java/com/alibaba/dt/graph and  /src/main/java/com/alibaba/dt/udf respectively.

2) upload the four SQL-files as resources on MaxCompute, and create a Shell-Node (/src/main/java/com/alibaba/dt/start.sh).
   The input of the Shell-Node is a table with 3 columns, named as "src_guid", "dst_guid", "ratio".
