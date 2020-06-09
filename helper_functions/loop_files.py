import glob
import os


def loop_files(file_path, file_extension):
    """returns all files matching extension from a directory"""
    all_files = []
    for root, dirs, files in os.walk(file_path):
        files = glob.glob(os.path.join(root, file_extension))
        for f in files:
            all_files.append(os.path.abspath(f))

    return all_files
