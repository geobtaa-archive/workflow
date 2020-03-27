#!/usr/bin/env python3
import os
import fnmatch

# local imports
from solr_interface import SolrInterface
import config

### change this variable to point to the path where the json files are ###
PATH_TO_JSON_FOLDER = r" "

def get_files_from_path(start_path, criteria="*"):
    files = []

    for path, folder, ffiles in os.walk(start_path):
        for i in fnmatch.filter(ffiles, criteria):
            files.append(os.path.join(path, i))

    return files

def add_json(path_to_json, solr_instance):
    files = get_files_from_path(path_to_json, criteria="*.json")
    dicts = []

    for i in files:
        dicts.append(solr_instance.json_to_dict(i))

    if solr_instance.add_dict_list_to_solr(dicts):
        print("Added {n} records to solr.".format(n=len(dicts)))

def main():
    if config.SOLR_USERNAME and config.SOLR_PASSWORD:
        solr_url = config.SOLR_URL.format(
            username=config.SOLR_USERNAME,
            password=config.SOLR_PASSWORD
        )

    else:
        print("No username or password. Check config.py")
        return

    solr = SolrInterface(url=solr_url)

    if os.path.isdir(PATH_TO_JSON_FOLDER):
        add_json(PATH_TO_JSON_FOLDER, solr)

    else:
        print("JSON path is not valid. Please check the PATH_TO_JSON_FOLDER variable")
        return

if __name__ == "__main__":
    main()