#
#
#

#! /usr/bin/env python
import os
from glob import glob
import json
import sys
from collections import OrderedDict
import argparse
import pdb
import pprint
import time
import logging
import fnmatch
import urllib

# non-standard libraries.
from lxml import etree
from owslib import csw
from owslib.etree import etree
from owslib import util
from owslib.namespaces import Namespaces
import unicodecsv as csv
# demjson provides better error messages than json
import demjson
import requests

# local imports
from solr_interface import SolrInterface
import config

# from users import USERS_INSTITUTIONS_MAP

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

    def __init__(self, SOLR_URL, SOLR_USERNAME, SOLR_PASSWORD, INST=None,
                 max_records=None, COLLECTION=None, UUID=None, PUBLISHER=None):

        if SOLR_USERNAME and SOLR_PASSWORD:
            SOLR_URL = SOLR_URL.format(
                username=SOLR_USERNAME,
                password=SOLR_PASSWORD
            )

        self.solr = SolrInterface(log=log, url=SOLR_URL)

        self.inst = INST
        self.records = OrderedDict()
        self.record_dicts = OrderedDict()
        self.max_records = max_records
        self.uuid = UUID

        if COLLECTION:
            self.collection = '"' + COLLECTION + '"'
        else:
            self.collection = None

        self.institutions_test = {
            "minn": '"Minnesota"'
        }
        self.institutions = {
            "iowa": '"Iowa"',
            "illinois": '"Illinois"',
            "minn": '"Minnesota"',
            "psu": '"Penn State"',
            "msu": '"Michigan State"',
            "mich": '"Michigan"',
            "purdue": '"Purdue"',
            "umd": '"Maryland"',
            "wisc": '"Wisconsin"',
            "indiana": '"Indiana"',
            "stanford": '"Stanford"',
            "uva": '"UVa"',
            "nyu": '"Baruch CUNY"',
            "col": '"Columbia"'

        }

        self.identifier = {
            "id": '"enterUUIDhere"'


        }

        self.publisher = {
            "idot": '"Iowa Department of Transportation"'


        }

        self.collection = {
            "arc": '"ArcGIS Open Data"',
            "01c-01": '"Bloomington Open Data"',
			"01c-02": '"Indianapolis Open Data"',
			"01d-01": '"Indiana Historic Maps"',
			"02a-01": '"Illinois Geospatial Data Clearinghouse"',
			"02c-01": '"City of Chicago Data Portal"',
			"03a-01": '"IowaDNR Infrastructure Records"',
			"03a-02": '"IowaDOT"',
			"03a-03": '"Iowa Geodata"',
			"03d-01": '"Iowa Historical County Atlases"',
			"03d-02": '"Hixson Plat Map Atlases of Iowa"',
			"04a-01": '"Maryland iMap"',
			"04c-01": '"District of Columbia Open Data"',
			"04c-02": '"Baltimore Open Data"',
			"04c-03": '"Open Baltimore"',
			"04d-01": '"University of Maryland Digital Collections"',
			"04d-02": '"Maryland Scanned Foreign Maps"',
			"05a-01": '"Minnesota Geospatial Commons"',
			"05b-99": '"Minnesota Counties"',
			"05a-02": '"MN Legislature"',
			"05b-01": '"Anoka County"',
			"05b-02": '"Becker County"',
			"05b-03": '"Carver County"',
			"05b-04": '"Chisago County"',
			"05b-05": '"Clay County"',
			"05b-06": '"Hennepin County"',
			"05b-07": '"Otter Tail County"',
			"05b-09": '"St Louis County"',
			"05b-10": '"Scott County"',
			"05b-11": '"Stearns County"',
			"05b-12": '"Washington County"',
			"05b-13": '"Carlton County MN"',
			"05c-01": '"Minneapolis Open Data"',
			"05d-01": '"John R. Borchert Map Library"',
			"05d-02": '"Borchert Sheet maps"',
			"05d-03": '"DRUM"',
			"05d-04": '"Minnesota Geological Survey"',
			"06a-01": '"State of Michigan Open Data Portal"',
			"06a-02": '"Michigan DNR"',
			"06a-03": '"Michigan DOT"',
			"06d-01": '"Michigan State University Libraries Maps"',
			"07c-01": '"Detroit Open Data"',
			"07c-02": '"City of Ann Arbor"',
			"07d-01": '"Clark Library Scanned Maps"',
			"07e-01": '"SEMCOG"',
			"08a-01": '"Pennsylvania Spatial Data Access (PASDA)"',
			"08d-01": '"Penn State Rare Maps"',
			"08d-02": '"Sanborn Fire Insurance Maps"',
			"09a-01": '"IndianaMAP"',
			"09d-01": '"Purdue Georeferenced Imagery"',
			"10a-01": '"State of Wisconsin"',
			"10a-02": '"WI DATCP"',
			"10a-03": '"WI DNR"',
			"10a-04": '"WI LTSB"',
			"10a-05": '"WI LiDAR/Imagery"',
			"10a-05": '"Wisconsin DNR"',
			"10b-01": '"WI Counties"'
        }


    def delete_records_institution(self, inst):
        """
        Delete records from Solr.

        """
        self.solr.delete_query("dct_provenance_s:" + self.institutions[inst])

    def delete_one_record(self, uuid):

    	self.solr.delete_query("uuid:" + self.identifier[uuid])

    def delete_records_collection(self, collection):
        """
        Delete records from Solr.
        """
        self.solr.delete_query("dct_isPartOf_sm:" + self.collection[collection])

    def delete_records_publisher(self, publisher):
        """
        Delete records from Solr.
        """
        self.solr.delete_query("dc_publisher_sm:" + self.publisher[publisher])


    def update_one_record(self, uuid):
        url = self.CSW_URL.format(virtual_csw_name="publication")
        self.connect_to_csw(url)
        self.csw_i.getrecordbyid(
            id=[uuid],
            outputschema="http://www.isotc211.org/2005/gmd"
        )
        self.records.update(self.csw_i.records)
        rec = self.records[uuid].xml
        rec = rec.replace("\n", "")
        root = etree.fromstring(rec)
        record_etree = etree.ElementTree(root)
        inst = self.get_inst_for_record(uuid)
        result = self.transform(
            record_etree,
            institution=self.institutions[inst]
        )
        result_u = unicode(result)
        log.debug(result_u)
        try:
            self.record_dicts = OrderedDict({uuid: demjson.decode(result_u)})
            log.debug(self.record_dicts)
        except demjson.JSONDecodeError as e:
            log.error("ERROR: {e}".format(e=e))
            log.error(result_u)

        self.handle_transformed_records()

    @staticmethod
    def chunker(seq, size):
        if sys.version_info.major == 3:
            return (seq[pos:pos + size] for pos in range(0, len(seq), size))
        elif sys.version_info.major == 2:
            return (seq[pos:pos + size] for pos in xrange(0, len(seq), size))

    def handle_transformed_records(self, output_path="./output"):
        log.debug("Handling {n} records.".format(n=len(self.record_dicts)))

        if self.to_csv:
            self.to_spreadsheet(self.record_dicts)

        elif self.to_json:
            self.output_json(output_path)

        elif self.to_xml:
            self.output_xml(output_path)

        elif self.to_xmls:
            self.single_xml(output_path)

        elif self.to_opengeometadata:
            self.output_json(self.to_opengeometadata)
            self.output_xml(self.to_opengeometadata)
            self.output_layers_json(self.to_opengeometadata)

        else:
            self.solr.add_dict_list_to_solr(self.record_dicts.values())
            log.info("Added {n} records to Solr.".format(
                n=len(self.record_dicts)
            ))


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-pi",
        "--provenance-institution",
        help="The institution to assign dct_provenance_s to. If provided, this \
            will speed things up. But make sure it's applicable to _all_ \
            records involved. Valid values are one of the following : iowa, \
            illinois, mich, minn, msu, psu, purdue, umd, wisc")
    parser.add_argument(
        "-c",
        "--collection",
        help="The collection name (dc_collection) to use for these records. \
            Added as XSL param")

    output_group = parser.add_mutually_exclusive_group(required=False)


    group = parser.add_mutually_exclusive_group(required=True)

    group.add_argument(
        "-di",
        "--delete-records-institution",
        help="Delete records for an instution.\n\
              Valid values are one of the following : \
              iowa, illinois, mich, minn, msu, psu, purdue, umd, wisc")

    group.add_argument(
        "-dc",
        "--delete-records-collection",
        help="Delete records for an collection.\n\
              Enter the collection name")

    group.add_argument(
        "-ds",
        "--delete-one-record")

    args = parser.parse_args()
    interface = CSWToGeoBlacklight(
        config.SOLR_URL, config.SOLR_USERNAME, config.SOLR_PASSWORD,
        INST=args.provenance_institution,
        COLLECTION=args.collection)


    if args.delete_records_institution:
        interface.delete_records_institution(args.delete_records_institution)

    elif args.delete_records_collection:
        interface.delete_records_collection(args.delete_records_collection)

    elif args.delete_one_record:
        interface.delete_one_record(args.delete_one_record)

    elif args.delete_publisher:
        interface.delete_publisher(args.delete_publisher)

    else:
        sys.exit(parser.print_help())



if __name__ == "__main__":
    sys.exit(main())
