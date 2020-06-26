Data Pipelines:-
---------------------------
Three Airflow DAGs are developed to load data through the different layers of the Data Lake:-

1- load_landing_zone : loads the data from S3 buckets to the Landing zone on the EMR Cluster, runs monthly & triggers load_integration_layer DAG
<<attach DAG Image here>>

2- load_integration_layer : 
- Loads the data to the Integration Layer on the EMR cluster, this layer is where all data cleaning data integration/delta load activities are performed with the same structure as the sources.
- A data counting is performed for all the tables in this layer, also NULLs check will be included in this DAG
<<attach DAG Image here>>

3- load_presentation_layer : 
- loads the data to the Presentation Layer, Presentation Layer contains the required data structures/models to be consumed by other teams eg data analysts, data scientist & reporting
- A data counting check & a NULLs check are added to the end of this DAG to warn in case of any data discrepancies
<<attach DAG Image here>>


Data Models:-
-----------------------------
- edge_zone (this will be renamed to s3_layer)
- landing_zone
- integration_layer
- presentation_layer

# Flight-DWH-for-Data-Analytics-and-Predictive-Modeling
Building DWH Layers to serve as consolidated source for data analytics and predictive modeling of USA flights data

- this should be renamed to data Lake not DWH

- DL to serve as a unified data source for different teams, Reporting, anlytics & data Science teams

- Creation of Logical layers/Zones,Edge, Landing, Staging/Integration & Conformed/analytical/Presentation
- all these layers are simply local file system of Ubuntu, hich would be migrated to HDFS in another phase & ultimately to an EMR cluster
- edge is where the src files exist
- Landing layer is where all external tables are created to read from src files on Edge node
- staging layer is where a 3NF model is created with consistent schema & curated data
- conformed layer is where Anlytical, Reporting & Data science tables created for different use cases

### Environment setup/requiremetns:-
-------------
- Ubuntu/*nix OS
- Spark binaries 

- delta lake jar

### Data Loading strategies :-
-----------
Landing zone : overwrite insert, as it contains only the most recent loaded data
Integration Zone : for Lookups Merge, for Facts Delete Insert, as each month should be loaded individually, and should be loaded once, also fact tables should be paritioned


- EC2 Deployment ?

Github Project Name : Multi-Layer Data Lake Pipelines on AWS 

Github Project Description : Building a multi-layer data lake on AWS

Project summary/Objective:-
-------------------------------------
- Create a scalable & accessible central data lake to provide data with different  





# Improvements :-
-----------------------
- provision an EC2 instance to be a central node accessible for team members and manages airflow 
- automate the process of loading the data on S3
- Create a Raw zone with no defined schema to have schema on read advantage
- use copy command to load the data on the Raw zone
- Retention Plan
- Provision a MySQL db to be be the metastore for spark and airflow
# TODO : Consolidate all repetitive queries into spark handler class, (Db creation and drop)
# TODO : Enable Mysql Connection for Metastore db
# TODO : Provision an EMR Cluster for just one lookup, read data from S# and load it through the Layers
# The final Model may not be suitable for data lakes, but i chose to make it in the lowest granularity to be able to practice more on data handling
# TODO : create a drop table function in IL & PL to delete HDFS Directory and drop table in SPARK SQL
# TODO : raise exception in case of and error in all load functions to mark airflow task as failed
# hdfs_path = os.environ['HDFS_PATH']
# edge_node_path = os.environ['EDGE_NODE_PATH']
# TODO : Data should be moved into and archive after loaded in INTEGRATION_LAYER
# TODO : Include Null checks in Integartion Layer DAG