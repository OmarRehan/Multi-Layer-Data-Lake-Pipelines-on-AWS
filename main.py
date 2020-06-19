# TODO : Consolidate all repetitive queries into spark handler class, (Db creation and drop)
# TODO : Functionalize all create load files
# TODO : Enable Mysql Connection for Metastore db
# TODO : Provision an EMR Cluster for just one lookup, read data from S# and load it through the Layers
# The final Model may not be suitable for data lakes, but i chose to make it in the lowest granularity to be able to practice more on data handling
# TODO : create a drop table function in IL & PL to delete HDFS Directory and drop table in SPARK SQL
# TODO : raise exception in case of and error in all load functions to mark airflow task as failed
# hdfs_path = os.environ['HDFS_PATH']
# edge_node_path = os.environ['EDGE_NODE_PATH']
