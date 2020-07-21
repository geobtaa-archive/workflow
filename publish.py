original
#This script will add geoblacklight json files to Solr from a folder of nested files.
#check configSolr to change from dev to public server
#To run, type: publish.py -aj foldername

#! /usr/bin/env python
import os
from glob import glob
import json
import sys
# from collections import OrderedDict
import argparse
import pdb
# import pprint
import time
import logging
import fnmatch
import urllib

# non-standard libraries.
from lxml import etree
# from owslib import csw
from owslib.etree import etree
from owslib import util
from owslib.namespaces import Namespaces
# import unicodecsv as csv
# demjson provides better error messages than json
import demjson
import requests

# local imports
from solr_interface import SolrInterface
import config


# logging stuff
if config.DEBUG:
    log_level = logging.DEBUG
else:
    log_level = logging.INFO
global log
log = logging.getLogger('owslib')
log.setLevel(log_level)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(log_level)
log_formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
ch.setFormatter(log_formatter)
log.addHandler(ch)



class CSWToGeoBlacklight(object):

    def __init__(self, SOLR_URL, SOLR_USERNAME, SOLR_PASSWORD,
                 max_records=None):

        if SOLR_USERNAME and SOLR_PASSWORD:
            SOLR_URL = SOLR_URL.format(
                username=SOLR_USERNAME,
                password=SOLR_PASSWORD
            )

        self.solr = SolrInterface(log=log, url=SOLR_URL)
#         self.records = OrderedDict()
#         self.record_dicts = OrderedDict()
#         self.max_records = max_records


    def get_files_from_path(self, start_path, criteria="*"):
        files = []

        # self.RECURSIVE:
        for path, folder, ffiles in os.walk(start_path):
            for i in fnmatch.filter(ffiles, criteria):
                files.append(os.path.join(path, i))
        # else:
        #     files = glob(os.path.join(start_path, criteria))
        return files

    def add_json(self, path_to_json):
        files = self.get_files_from_path(path_to_json, criteria="*.json")
        log.debug(files)
        dicts = []
        for i in files:
            dicts.append(self.solr.json_to_dict(i))
        self.solr.add_dict_list_to_solr(dicts)
        log.info("Added {n} records to solr.".format(n=len(dicts)))


def main():
    parser = argparse.ArgumentParser()

    output_group = parser.add_mutually_exclusive_group(required=False)

    group = parser.add_mutually_exclusive_group(required=True)

    group.add_argument(
        "-aj",
        "--add-json",
        help="Indicate path to folder with GeoBlacklight \
              JSON files that will be uploaded.")


    args = parser.parse_args()

    interface = CSWToGeoBlacklight(
        config.SOLR_URL, config.SOLR_USERNAME, config.SOLR_PASSWORD)

    if args.add_json:
        interface.add_json(args.add_json)

    else:
        sys.exit(parser.print_help())


if __name__ == "__main__":
    sys.exit(main())