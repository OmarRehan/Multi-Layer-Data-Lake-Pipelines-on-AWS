import logging
import configparser
import os


def read_configs_file():
    # Reading the configuration file
    try:
        config_path = os.environ['CONFIGS_PATH']
        config = configparser.ConfigParser()
        config.read(config_path)

        return config

    except Exception as e:
        logging.error(f"Failed to open the configuration file,{e}")


if __name__ == '__main__':
    read_configs_file()
