import logging
import configparser


def read_configs_file():
    # Reading the configuration file
    try:

        config = configparser.ConfigParser()
        config.read('/home/admin_123/Desktop/Main/Configs/flights_configs.cfg')
        # TODO : Add config file path to activate file in the virtual env

        return config

    except Exception as e:
        logging.error(f"Failed to open the configuration file,{e}")
