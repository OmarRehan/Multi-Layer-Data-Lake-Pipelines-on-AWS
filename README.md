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