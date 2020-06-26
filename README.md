Project Summary:-
-----------------
The Objective of this project it to create a multi-layer, scalable & accessible Data Lake, to store & process big amounts to be a unified data source, that can be used by data analytics, reporting & data science teams.
The current demo is to handle data life cycle of USA flights through the different layers of the data lake.

Data Sources:-
-----------------
- USA flights data from 'bureau of transportation statistics' : https://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236 , the avaerage records count for each month is 500k to 650k, for this project i loaded the whole data with all the columns of 2019 
- USA City Demographics : https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/ .

System Components :-
------------------------
<<<attach system component image here>>>
- Delta Lake :-
    - provides ACID transactions, scalable metadata handling, and unifies streaming and batch data processing. 
    - Also Delta Lake is fully compatible with Apache Spark APIs so users with different skills can use the data whether using SQL (Spark SQL),spark APIs ,python, R, etc...
    - Delta Lake stores the data as parquet files, so data is still readable with all tools can read parquet like Hive 

- AWS EMR:-
    - AWS EMR provides All tools needed for Percussing,Storage, Resource Management & Data Access tools as PAAS.

- S3 :-
    - S3 is the current source of data, this is the first interaction with the data in the current scope, as Airflow DAGs will consume the data from it into the other layers in the data lake.
    
- Apache Airflow :-
    - Airflow acts as the main orchestration tool for the data lake as it provides many required features for the current scope eg: back filling, web UI, scheduling, etc...
    
Data Layers, Models & Loading Strategies:-
------------------
S3 Bucket:
---------
- Contains the files without any modifications except for non supported files formats.
- The data uploaded manually for now, it will be automated but it is out f the current scope


- The Storage Layer in the data lake is built using HDFS, It consists of 3 Separated data Layers.
- Each data layer is represented as a Spark SQL Database and accessible using all Spark APIs & any tool that can parse parquet files.

Landing Zone:-
-------------
- Contains the most recent data from S3.
- Initial & Incremental load are overwrite.
- Contains the data as it is from the sources.
- Accepts all HDFS supported files formats.
- Relations in the Model are not enforced as these are just files, it is just to depict how to integrate the data together.

<<<attach model here>>>

Integration Layer:-
-------------
- Contains all the data from sources with the same structure as the sources but data is cleaned eg: handling missing data, data types are defined, enforcing the relations among the tables, etc...
- Initial & Incremental load differs according to each table's type, for lookups/assets tables it is Merge/SCD1 handling, for transactional tables it is overwrite based on Airflow execution date so it deletes a specific partition if exists then inserts the data, each table's loading strategies is in the mapping sheet.
- Data stored as Delta Lake format to support ACID, Merge and many other Data Lake features. 
- Relations in the Model are enforced during the data processing

<<<attach model here>>>


Presentation Layer:-
-------------
- This Layer contains models required by data analytics, reporting & data science teams so they can build models, derive insights & generate reports.
- Initial & Incremental load differs according to each table's type, each table's loading strategies is in the mapping sheet.
- Data stored as Delta Lake format to support ACID, Merge and many other Data Lake features. 
- Model Node : the below model is just for the sake of demo and to have as much data transformation as possible, as source data is denormalized, i had to make a normalized model which does not conform completely with data lake concepts.

<<<attach model here>>>

Data Pipelines:-
---------------------------
Three Airflow DAGs are developed to load data through the different layers of the Data Lake:-

1- load_landing_zone : 

- Loads the data from S3 bucket to the Landing zone on the EMR Cluster
- Runs monthly & triggers load_integration_layer DAG

<<attach DAG Image here>>

2- load_integration_layer : 
- Loads the data to the Integration Layer on the EMR cluster, this layer is where all data cleaning data integration/delta load activities are performed with the same structure as the sources.
- A data counting quality check is performed for all the tables in this layer, also NULLs check will be included in this DAG

<<attach DAG Image here>>
<<attach Counts image>>

3- load_presentation_layer : 
- loads the data to the Presentation Layer, Presentation Layer contains the required data structures/models to be consumed by other teams eg data analysts, data scientist & reporting
- A data counting check & a NULLs check are added to the end of this DAG to warn in case of any data discrepancies
<<attach DAG Image here>>
<<attach Counts image>>
<<attach Nulls image>>

Environment Setup:-
------------------
An Airflow DAG (setup_cloud_environment.py) is developed to setup AWS environment, it creates S3 Bucket & Directories, start, configure EMR cluster and creates Spark SQL databases.

To be able to execute the DAG a machine with Airflow installed and some defined environment variables are required.

- export AWS_ACCESS_KEY_ID=<<AWS access key should be defined>>
- export AWS_SECRET_ACCESS_KEY=<<AWS secret key should be defined>>
- export EC2_KEY_NAME=<<EC2 key name>>
- export EC2_VPC_SUBNET=<<Security group's vpc subnet>>
- Warning, delta-core_2.11-0.6.1.jar needs to be uploaded to "s3://<<S3 Bucket name>>/BOOTSTRAP_ACTIONS/" before executing the DAG, this step will be added in the DAG the next version

<<attach setup_aws_environement DAG here>>


# Improvements (in progress):-
-----------------------
system Improvements:

- provision an EC2 instance to be a central node accessible for team members and manages airflow 
- Provision a MySQL db to be be the metastore for Spark and Airflow

Data Management Improvements:

- implement a Retention Plan
- Automate the process of uploading the data on S3
- Create a Raw zone with no defined schema to have schema on read advantage in case of changes structures of any tables
- use copy command to load the data on the Raw zone from S3


Development Improvements:
- Consolidate all repetitive queries into spark handler class, (Db creation and drop)
- Create a drop table function in IL & PL to delete HDFS Directory and drop table in SPARK SQL
- Raise exception in case of and error in all load functions to mark airflow task as failed
- Include Null checks in Integration Layer DAG