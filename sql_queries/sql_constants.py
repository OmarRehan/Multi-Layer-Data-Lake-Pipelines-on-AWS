import os


hdfs_path = os.environ['HDFS_PATH']
edge_node_path = os.environ['EDGE_NODE_PATH']
# Local Paths
# hdfs_path = 'hdfs://localhost:9000'
# edge_node_path = '/home/admin_123/Desktop/Main/DataSets/FlightsData/'

dict_dbs_names = {
    'LANDING_ZONE_NAME': 'LANDING_ZONE',
    'INTEGRATION_LAYER_NAME': 'INTEGRATION_LAYER',
    'PRESENTATION_LAYER_NAME': 'PRESENTATION_LAYER'
}


dict_dbs_locations = {
    'LANDING_ZONE_LOC': f'{hdfs_path}/FLIGHTS_DL/LANDING_ZONE',
    'INTEGRATION_LAYER_LOC': f'{hdfs_path}/FLIGHTS_DL/INTEGRATION_LAYER',
    'PRESENTATION_LAYER_LOC': f'{hdfs_path}/FLIGHTS_DL/PRESENTATION_LAYER'
}

missing_val_replace_numeric = -9999
missing_val_replace_alphanumeric = '~UNKNOWN~'
