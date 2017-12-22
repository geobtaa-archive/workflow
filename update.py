#Script for updating values in ISO 19139 XML files residing in an instance of GeoNetwork.
#Created for Big Ten Academic Alliance Geospatial Data Project
#2016
#type: python update.py filename.csv



#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

# Python standard libs
import logging
import os
import sys
import json
from datetime import datetime
import argparse
import time
import pdb

# non standard dependencies
from owslib import csw
from owslib.etree import etree
from owslib import util
from owslib.namespaces import Namespaces
import unicodecsv as csv
from dateutil import parser
# from xml.etree import ElementTree as etree

# config options - see config.py.sample for how to structure
from config import CSW_URL_PUB, CSW_USER, CSW_PASSWORD, DEBUG

# logging
if DEBUG:
    log_level = logging.DEBUG
else:
    log_level = logging.INFO
log = logging.getLogger('owslib')
log.setLevel(log_level)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(log_level)
log_formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(log_formatter)
log.addHandler(ch)


class UpdateCSW(object):

    def __init__(self, url, username, password, input_csv_path):
        self.INNER_DELIMITER = "###"
        self.GEMET_ANCHOR_BASE_URI = "https://geonet.lib.umn.edu:80/geonetwork/srv/eng/xml.keyword.get?thesaurus=external.theme.gemet-en&id="
        self.csw = csw.CatalogueServiceWeb(
            url, username=username, password=password)
        self.records = {}

        if not os.path.isabs(input_csv_path):
            input_csv_path = os.path.abspath(
                os.path.relpath(input_csv_path, os.getcwd()))

        self.csvfile = open(input_csv_path, "rU")

        self.reader = csv.DictReader(self.csvfile)
        self.fieldnames = self.reader.fieldnames

        self.namespaces = self.get_namespaces()

        # these are the column names that will trigger a change
        self.field_handlers = {"iso19139": {
            "NEW_title": self.NEW_title,
            "NEW_publisher": self.NEW_publisher,
            "NEW_originator":self.NEW_originator,
            "NEW_distributor":self.NEW_distributor,
            "NEW_link_download": self.NEW_link_download,
            "NEW_link_service_wms": self.NEW_link_service_wms,
            "NEW_link_service_esri": self.NEW_link_service_esri,
            "NEW_link_information": self.NEW_link_information,
            "NEW_link_metadata": self.NEW_link_metadata,
            "NEW_distribution_format": self.NEW_distribution_format,
            "NEW_topic_categories": self.NEW_topic_categories,
            "NEW_abstract": self.NEW_abstract,
            "NEW_keywords_theme": self.NEW_keywords_theme,
            "NEW_keywords_theme_gemet_name": self.NEW_keywords_theme_gemet_name,
            "NEW_keywords_place": self.NEW_keywords_place,
            "NEW_keywords_place_geonames": self.NEW_keywords_place_geonames,
            "NEW_date_publication": self.NEW_date_publication,
            "NEW_date_revision": self.NEW_date_revision,
            "NEW_temporal_end": self.NEW_temporal_end,
            "NEW_temporal_start": self.NEW_temporal_start,
            "NEW_temporal_instant": self.NEW_temporal_instant,
            "DELETE_link": self.DELETE_link,
            "DELETE_link_no_protocol": self.DELETE_link_no_protocol,
			"NEW_ref_system": self.NEW_ref_system,
			"NEW_north_extent": self.NEW_north_extent,
			"NEW_south_extent": self.NEW_south_extent,
			"NEW_east_extent": self.NEW_east_extent,
			"NEW_west_extent": self.NEW_west_extent,
			"NEW_collective_title": self.NEW_collective_title,
			"NEW_other_citation": self.NEW_other_citation,
			"NEW_maintenance_note": self.NEW_maintenance_note,
			"NEW_parent": self.NEW_parent,
			"NEW_dataset_uri": self.NEW_dataset_uri,
            "NEW_identifier": self.NEW_identifier,
            "NEW_provenance": self.NEW_provenance,
            "NEW_credit": self.NEW_credit,
            "NEW_thumbnail":self.NEW_thumbnail

        },


		#the xpaths to all of the elements accessible for changes.
		#Root is gmd:MD_Metadata
        }
        self.XPATHS = {"iso19139": {
            "citation": "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation",
            "publisher": "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:citedResponsibleParty/gmd:CI_ResponsibleParty[gmd:role/gmd:CI_RoleCode[@codeListValue='publisher']]/gmd:organisationName/gco:CharacterString",
            "originator": "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:citedResponsibleParty/gmd:CI_ResponsibleParty[gmd:role/gmd:CI_RoleCode[@codeListValue='originator']]/gmd:organisationName/gco:CharacterString",
            "title": "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:title/gco:CharacterString",
            "distribution_format": "gmd:distributionInfo/gmd:MD_Distribution/gmd:distributionFormat/gmd:MD_Format/gmd:name/gco:CharacterString",
            "date_creation": "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:date/gmd:CI_Date/gmd:dateType/gmd:CI_DateTypeCode[@codeListValue='creation']",
            "date_publication": "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:date/gmd:CI_Date/gmd:dateType/gmd:CI_DateTypeCode[@codeListValue='publication']",
            "date_revision": "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:date/gmd:CI_Date/gmd:dateType/gmd:CI_DateTypeCode[@codeListValue='revision']",
            "contact_organization": "gmd:contact/gmd:CI_ResponsibleParty/gmd:organisationName/gco:CharacterString",
            "contact_individual": "gmd:contact/gmd:CI_ResponsibleParty/gmd:individualName/gco:CharacterString",
            "timestamp": "gmd:dateStamp",
            "md_distribution": "gmd:distributionInfo/gmd:MD_Distribution",
            "distributor": "gmd:distributionInfo/gmd:MD_Distribution/gmd:distributor/gmd:MD_Distributor/gmd:distributorContact/gmd:CI_ResponsibleParty[gmd:role/gmd:CI_RoleCode[@codeListValue='distributor']]/gmd:organisationName/gco:CharacterString",
            "transferOptions": "gmd:distributionInfo/gmd:MD_Distribution/gmd:transferOptions",
            "digital_trans_options": "gmd:distributionInfo/gmd:MD_Distribution/gmd:transferOptions/gmd:MD_DigitalTransferOptions",
            "online_resources": "//gmd:transferOptions/gmd:MD_DigitalTransferOptions/gmd:onLine/gmd:CI_OnlineResource",
            "online_resource_links": "//gmd:transferOptions/gmd:MD_DigitalTransferOptions/gmd:onLine/gmd:CI_OnlineResource/gmd:linkage/gmd:URL",
            "link_no_protocol": "//gmd:transferOptions/gmd:MD_DigitalTransferOptions/gmd:onLine/gmd:CI_OnlineResource[not(gmd:protocol)]",
            "distribution_link": "gmd:distributionInfo/gmd:MD_Distribution/gmd:transferOptions[{index}]/gmd:MD_DigitalTransferOptions[1]/gmd:onLine[1]/gmd:CI_OnlineResource[1]/gmd:linkage[1]/gmd:URL[1]",
            "distributor_distribution_link": "gmd:distributionInfo/gmd:MD_Distribution/gmd:distributor[{distributor_index}]/gmd:MD_Distributor/gmd:distributorTransferOptions[{index}]/gmd:MD_DigitalTransferOptions/gmd:onLine/gmd:CI_OnlineResource/gmd:linkage/gmd:URL",
            "keywords_theme": "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:descriptiveKeywords/gmd:MD_Keywords/gmd:type/gmd:MD_KeywordTypeCode[@codeListValue='theme']/../../gmd:keyword/gco:CharacterString",
            "keywords_theme_base": "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:descriptiveKeywords/gmd:MD_Keywords/gmd:type/gmd:MD_KeywordTypeCode[@codeListValue='theme']",
            "keywords_theme_gemet": "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:descriptiveKeywords/gmd:MD_Keywords/gmd:thesaurusName/gmd:CI_Citation/gmd:title/gco:CharacterString[text()='GEMET']",
            "keywords_place": "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:descriptiveKeywords/gmd:MD_Keywords/gmd:type/gmd:MD_KeywordTypeCode[@codeListValue='place']/../../gmd:keyword/gco:CharacterString",
            "keywords_place_base": "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:descriptiveKeywords/gmd:MD_Keywords/gmd:type/gmd:MD_KeywordTypeCode[@codeListValue='place']",
            "keywords_place_geonames": "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:descriptiveKeywords/gmd:MD_Keywords/gmd:thesaurusName/gmd:CI_Citation/gmd:title/gco:CharacterString[text()='GeoNames']",
            "descriptive_keywords": "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:descriptiveKeywords",
            "md_data_identification": "gmd:identificationInfo/gmd:MD_DataIdentification",
            "topic_categories": "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:topicCategory/gmd:MD_TopicCategoryCode",
            "abstract": "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:abstract/gco:CharacterString",
            "purpose": "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:purpose/gco:CharacterString",
            "extent": "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent",
            "temporalextent": "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:temporalElement",
            "temporalextent_start": "gmd:EX_TemporalExtent/gmd:extent/gml:TimePeriod/gml:beginPosition",
            "temporalextent_end": "gmd:EX_TemporalExtent/gmd:extent/gml:TimePeriod/gml:endPosition",
            "temporalextent_instant": "gmd:EX_TemporalExtent/gmd:extent/gml:TimeInstant/gml:timePosition",
            "ci_date_type": "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:date/gmd:CI_Date[gmd:dateType/gmd:CI_DateTypeCode/@codeListValue='{datetype}']/gmd:date",
            "ref_system": "gmd:referenceSystemInfo/gmd:MD_ReferenceSystem/gmd:referenceSystemIdentifier/gmd:RS_Identifier/gmd:code/gco:CharacterString",
            "north_extent": "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicBoundingBox/gmd:northBoundLatitude/gco:Decimal",
            "south_extent": "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicBoundingBox/gmd:southBoundLatitude/gco:Decimal",
            "east_extent": "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicBoundingBox/gmd:eastBoundLongitude/gco:Decimal",
            "west_extent": "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicBoundingBox/gmd:westBoundLongitude/gco:Decimal",
            "collective_title": "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:collectiveTitle/gco:CharacterString",
            "other_citation": "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:otherCitationDetails/gco:CharacterString",
            "maintenance_note": "gmd:metadataMaintenance/gmd:MD_MaintenanceInformation/gmd:maintenanceNote/gco:CharacterString",
            "dataset_uri": "gmd:dataSetURI/gco:CharacterString",
            "parent": "gmd:parentIdentifier/gco:CharacterString",
            "identifier": "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:identifier/gmd:MD_Identifier/gmd:code/gco:CharacterString",
            "credit": "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:credit",
            "provenance": "gmd:metadataMaintenance/gmd:MD_MaintenanceInformation/gmd:updateScopeDescription/gmd:MD_ScopeDescription/gmd:other/gco:CharacterString",
            "thumbnail": "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:graphicOverview/gmd:MD_BrowseGraphic/gmd:fileName/gco:CharacterString"

        },

		#protocols for types of links
        }
        self.protocol_map = {
            "download": ["WWW:DOWNLOAD-1.0-ftp--download",
                         "download",
                         "WWW:DOWNLOAD-1.0-http--download",
                         "WWW:DOWNLOAD"],
            "information": ["WWW:LINK",
                            "WWW:LINK-1.0-http--link"],
            "esri_service": ["ESRI:ArcGIS"],
            "wms_service": ["OGC:WMS"],
            "wfs_service": ["OGC:WFS"],
            "wcs_service": ["OGC:WCS"],
            "metadata": ["metadata"]
        }

		#Topic categories.  These need to be spelled and cased as below to register in GeoNetwork
        self.topic_categories = [
            'Intelligence military', 'environment',
            'Geoscientific information', 'elevation', 'Utilities communications',
            'structure', 'oceans', 'Planning cadastre', 'Inland waters',
            'boundaries', 'society', 'biota', 'health', 'location',
            'Climatology, meteorology, atmosphere', 'transportation', 'farming',
            'Imagery base maps earth cover', 'economy']



    @staticmethod
    def get_namespaces():
        """
        Returns specified namespaces using owslib Namespaces function.
        """
        n = Namespaces()
        ns = n.get_namespaces(
            ["gco", "gmd", "gml", "gml32", "gmx", "gts", "srv", "xlink", "dc"])
        return ns

    @staticmethod
    def _filter_link_updates_or_deletions(field):
        """
        Filter function used to identify link updates or deletions from the CSV
        """

        if field.startswith("NEW_link") or field.startswith("DELETE_link"):
            return True

    def _get_links_from_record(self, uuid):
        """
        Sets self.record_online_resources to a list of all\
        CI_OnlineResource elements
        """
        self.record_online_resources = self.record_etree.findall(
            self.XPATHS[self.schema]["online_resources"], self.namespaces)



#PRIMARY FUNCTIONS FOR UPDATING AND CREATING ELEMENTS
    def _simple_element_update(self, uuid, new_value, xpath=None, element=None):
        """
        Primary function for most records.
        Updates single element of record. Nothing fancy.
        Elements like abstract and title.
        Positional arguments:
        uuid -- the unique id of the record to be updated
        new_value -- the new value supplied from the csv
        Keyword arguments (need one and only one):
        xpath -- must follow straight from the root element
        element -- match a name in self.XPATHS for the current schema
        """

        if xpath:
            path = xpath
        elif element:
            path = self.XPATHS[self.schema][element]
        else:
            log.error("_simple_element_update: No xpath or element provided")
            return

        tree = self.record_etree

        original_path = path
        elem = []

		#looks for existence of path, if not there, moves up a level until an element node is present.
		#sets path based upon what exists.
		#will not work for missing base level elements that are not contained in another parent element beyond the root.

        while len(elem) == 0:
            elem = tree.xpath(path, namespaces=self.namespaces)
            if len(elem) == 0:
                log.debug(
                    "Did not find \n {p} \n trying next level up.".format(
                        p=path
                    )
                )
                path = "/".join(path.split("/")[:-1])

		#if there is an element ? and if the path exists
        if len(elem) > 0 and path == original_path:
            log.debug("Found the path: \n {p}".format(p=path))

			#checks to see if the text is the same as the new value, if not, changes
            if elem[0].text != new_value:
                elem[0].text = new_value
                self.tree_changed = True

			#if it is the same, just reports that
            else:
                log.info("Value for \n {p} \n already set to: {v}".format(
                    p=path.split("/")[-2], v=new_value))

		#if there is an element to be created but the path isn't there
        elif len(elem) > 0 and path != original_path:
            elements_to_create = [
                e for e in original_path.split("/") if e not in path]
            self._create_elements(elem[0], elements_to_create)
            log.debug(
                "Recursing to _simple_element_update now that the \
                element should be there.")
            self._simple_element_update(uuid, new_value, xpath=original_path)

	#called at the end of _simple_element_update if needed.
    def _create_elements(self, start_element, list_of_element_names):
        tree = self.record_etree
        base_element = start_element
        for elem_name in list_of_element_names:
            elem_name_split = elem_name.split(":")
            ns = "{" + self.namespaces[elem_name_split[0]] + "}"
            base_element = etree.SubElement(
                base_element,
                "{ns}".format(ns=ns) + elem_name_split[1],
                nsmap=self.namespaces
            )
            log.debug("Created {n}".format(n=elem_name))
            self.tree_changed = True


    def _base_element_update(self, uuid, new_value, xpath=None, element=None):
        """
        Trying to use for un-nested elements.
        similar to simple_element_update, but removed while statement.
        Will single element of record if the element exists.
        Will not create element.

        Positional arguments:
        uuid -- the unique id of the record to be updated
        new_value -- the new value supplied from the csv
        Keyword arguments (need one and only one):
        xpath -- must follow straight from the root element
        element -- match a name in self.XPATHS for the current schema
        """
        if xpath:
            path = xpath
        elif element:
            path = self.XPATHS[self.schema][element]
        else:
            log.error("_base_element_update: No xpath or element provided")
            return

        tree = self.record_etree

        original_path = path
        elem = []

		# looks for existence of path
		# does not work as needed yet 12-28-2016 km
        if len(elem) == 0:
            elem = tree.xpath(path, namespaces=self.namespaces)

            log.debug(
                    "Did not find \n {p} \n Setting root as path.".format(
                        p=path
                    )
                )

		#checks if there is an element ? and if the path exists
        if len(elem) > 0 and path == original_path:
            log.debug("Found the path: \n {p}".format(p=path))
			#checks to see if the text is the same as the new value, if not, changes
            if elem[0].text != new_value:
                elem[0].text = new_value
                self.tree_changed = True

		#if it is the same, just reports that
            else:
                log.info("Value for \n {p} \n already set to: {v}".format(
                    p=path.split("/")[-2], v=new_value))

       #if there is an element to be created but the path isn't there
        elif len(elem) > 0 and path != original_path:
            elements_to_create = [
                e for e in original_path.split("/") if e not in path]
            self._create_elements(elem[0], elements_to_create)
            log.debug(
                "Recursing to _simple_element_update now that the \
                element should be there.")
            self._base_element_update(uuid, new_value, xpath=original_path)


    #creates elements that can have multiple values
    #only for keywords
    def _make_new_multiple_element(self, element_name, value):
        # TODO abstract beyond keywords using element_name

        element = etree.Element("{ns}keyword".format(ns="{" + self.namespaces["gmd"] + "}"),
                                nsmap=self.namespaces)
        child_element = etree.SubElement(element,
                                         "{ns}CharacterString".format(ns="{" + self.namespaces["gco"] + "}"))
        child_element.text = value
        return element

    def _multiple_element_update(self, uuid, new_vals_string, multiple_element_name):
        """
        Keyword specific at the moment
        """
        log.debug("NEW VALUE INPUT: " + new_vals_string)
        new_vals_list = new_vals_string.split(self.INNER_DELIMITER)

        if len(new_vals_list) == 1 and new_vals_list[0] == "":
            return

        tree = self.record_etree
        tree_changed = False

        base_desc_kw = tree.findall(self.XPATHS[self.schema][multiple_element_name + "_base"],
                                    namespaces=self.namespaces)

        if len(base_desc_kw) == 0:
            # "descriptive_keywords"            :"gmd:identificationInfo/gmd:MD_DataIdentification/gmd:descriptiveKeywords",
            # "md_data_identification"          :"gmd:identificationInfo/gmd:MD_DataIdentification",
            new_desc_kw = self._parse_snippet(multiple_element_name + ".xml")
            existing_desc_kw = tree.findall(self.XPATHS[self.schema]["descriptive_keywords"],
                                            namespaces=self.namespaces)
            if len(existing_desc_kw) > 0:
                existing_desc_kw[-1].addnext(new_desc_kw)
                self.tree_changed = True
            else:
                md_data_identification = tree.find(self.XPATHS[self.schema]["md_data_identification"],
                                                   namespaces=self.namespaces)
                if md_data_identification is not None:
                    md_data_identification.append(new_desc_kw)
                    self.tree_changed = True
            log.debug(
                "Created descriptiveKeywords, now recursing to add keywords.")
            self._multiple_element_update(
                uuid, new_vals_string, multiple_element_name)

        xpath = self.XPATHS[self.schema][multiple_element_name]
        existing_vals = tree.findall(xpath, namespaces=self.namespaces)

        if len(existing_vals) > 0:
            md_keywords = existing_vals[0].getparent().getparent()
            existing_vals_list = [i.text for i in existing_vals]

            log.debug("EXISTING VALUES: " + "| ".join(existing_vals_list))
            add_values = list(set(new_vals_list) - set(existing_vals_list))
            delete_values = list(set(existing_vals_list) - set(new_vals_list))

            log.debug("VALUES TO ADD: " + "| ".join(add_values))
            log.debug("VALUES TO DELETE: " + "| ".join(delete_values))

        else:
            delete_values = []
            add_values = new_vals_list
            md_keywords = base_desc_kw[0].getparent().getparent()
            log.debug("VALUES TO ADD: " + ", ".join(add_values))

        for delete_value in delete_values:
            # TODO abstract out keyword specifics
            del_ele = tree.xpath(tree.getpath(
                md_keywords) + "/gmd:keyword/gco:CharacterString[text()='{val}']".format(val=delete_value), namespaces=self.namespaces)
            if len(del_ele) == 1:
                log.debug("Deleted: {v}".format(v=delete_value))
                p = del_ele[0].getparent()
                p.remove(del_ele[0])
                pp = p.getparent()
                pp.remove(p)
                tree_changed = True

        for value in add_values:

            new_element = self._make_new_multiple_element(
                multiple_element_name, value)
            md_keywords.append(new_element)
            tree_changed = True

        if tree_changed:
            self.tree_changed = True


# LINK UPDATE FUNCTIONS



    def _check_for_links_to_update(self, link_type):
        """
        Return a list of links that match a given type.
        Positional argument:
        link_type -- The type of link to look for. download, information,
            esri_service, and wms_service are current values for link_type)
        """
        self.protocols_list = self.protocol_map[link_type]
        links_to_update = filter(
            lambda resource,
            ns=self.namespaces,
            protocols=self.protocols_list: resource.findtext(
                "gmd:protocol/gco:CharacterString",
                namespaces=ns) in self.protocols_list,
            self.record_online_resources
        )
        return links_to_update

    def _add_protocol_to_resource(self, resource, link_type):
        """
        Creates a protocol element and its text for a given online resource.
        Positional arguments:
        resource -- A CI_Online_Resource currently lacking a protocol.
        link_type -- The type of link, which determines the protocol applied.
        """

        protocol_element = etree.SubElement(
            resource,
            "{ns}protocol".format(
                ns="{" + self.namespaces["gmd"] + "}"),
            nsmap=self.namespaces
        )
        char_string = etree.SubElement(
            protocol_element,
            "{ns}CharacterString".format(
               ns="{" + self.namespaces["gco"] + "}"),
            nsmap=self.namespaces
        )
        char_string.text = self.protocol_map[link_type][0]
        log.debug("Added protocol: {prot}".format(prot=char_string.text))

        # log.debug(etree.tostring(self.record_etree))
        return resource

    def _update_links_no_protocol(self, new_link, link_type, resources_no_protocol):
        """
        Matches inputted link to existing resources without
        protocols and if successful, adds protocol.
        Positional arguments:
        new_link -- The link to search for
        link_type -- The type of link, which us be used to create the protocol
        resources_no_protocol -- A list of OnlineResource Elements lacking protocol SubElements
        """

        for resource in resources_no_protocol:
            if resource.find(
                    "gmd:linkage/gmd:URL",
                    namespaces=self.namespaces).text == new_link:

                log.debug("updating resource with no protocol")
                self._add_protocol_to_resource(resource, link_type)
                # log.debug(etree.tostring(self.record_etree))
                self.tree_changed = True

    def _create_new_link(self, new_link, link_type):
        """
        Create a new onLine element.
        Assumes that gmd:MD_DigitalTransferOptions exists.
        Positional arguments:
        new_link -- The link to search for
        link_type -- The type of link, which us be used to create the protocol
        """

        transferOptions = self.record_etree.xpath(
            self.XPATHS[self.schema]["transferOptions"],
            namespaces=self.namespaces
        )

        new_link_layer_name = None
        new_link_split = new_link.split(self.INNER_DELIMITER)
        new_link = new_link_split[0]
        if len(new_link_split) > 1:
            new_link_layer_name = new_link_split[1]

        if len(transferOptions) > 0:
            digital_trans_options = self.record_etree.xpath(
                self.XPATHS[self.schema]["digital_trans_options"],
                namespaces=self.namespaces
            )

            if len(digital_trans_options) > 0:

                # create the elements
                online_element = etree.SubElement(
                    digital_trans_options[0],
                    "{ns}onLine".format(
                        ns="{" + self.namespaces["gmd"] + "}"),
                    nsmap=self.namespaces)
                ci_onlineresource = etree.SubElement(
                    online_element,
                    "{ns}CI_OnlineResource".format(
                        ns="{" + self.namespaces["gmd"] + "}"),
                    nsmap=self.namespaces)
                linkage = etree.SubElement(
                    ci_onlineresource,
                    "{ns}linkage".format(
                        ns="{" + self.namespaces["gmd"] + "}"),
                    nsmap=self.namespaces)
                url = etree.SubElement(
                    linkage,
                    "{ns}URL".format(
                        ns="{" + self.namespaces["gmd"] + "}"),
                    nsmap=self.namespaces)
                protocol = etree.SubElement(
                    ci_onlineresource,
                    "{ns}protocol".format(
                        ns="{" + self.namespaces["gmd"] + "}"),
                    nsmap=self.namespaces)
                protocol_string = etree.SubElement(
                    protocol,
                    "{ns}CharacterString".format(
                        ns="{" + self.namespaces["gco"] + "}"),
                    nsmap=self.namespaces)
                name = etree.SubElement(
                    ci_onlineresource,
                    "{ns}name".format(
                        ns="{" + self.namespaces["gmd"] + "}"),
                    nsmap=self.namespaces)
                name_string = etree.SubElement(
                    ci_onlineresource,
                    "{ns}CharacterString".format(
                        ns="{" + self.namespaces["gco"] + "}"),
                    nsmap=self.namespaces)

                # add the text
                url.text = new_link
                protocol_string.text = self.protocols_list[0]

                if new_link_layer_name:
                    name_string.text = new_link_layer_name

                self.tree_changed = True

                log.debug("created new link: {link}".format(link=new_link))
                # log.debug(etree.tostring(self.record_etree))
                # log.debug(self.csw.response)

        else:
            md_distribution = self.record_etree.xpath(self.XPATHS[self.schema]["md_distribution"],
                                                      namespaces=self.namespaces)
            if len(md_distribution) > 0:
                transfer_options = self._create_transferOptions(
                    md_distribution[0])
                self._create_md_digital_transfer_options(transfer_options)

                # recurse and try to make link again now that the parents are
                # in place
                log.debug("trying to create link again")
                self._create_new_link(new_link, link_type)

    def _create_transferOptions(self, md_distribution):
        return etree.SubElement(md_distribution,
                                "{ns}transferOptions".format(
                                    ns="{" + self.namespaces["gmd"] + "}"),
                                nsmap=self.namespaces)

    def _create_md_digital_transfer_options(self, transfer_options):
        return etree.SubElement(transfer_options,
                                "{ns}MD_DigitalTransferOptions".format(
                                    ns="{" + self.namespaces["gmd"] + "}"),
                                nsmap=self.namespaces)

    def _current_link_url_elements(self):
        return self.record_etree.xpath(
            self.XPATHS[self.schema]["online_resource_links"],
            namespaces=self.namespaces)

    def _current_link_urls(self):
        """
        Return a list of all URLs currently in the record.
        """

        links = self._current_link_url_elements()
        log.debug("Current link urls: " +
                  " | ".join([link.text for link in links]))
        return [link.text for link in links]

    def _get_resources_no_protocol(self):
        return self.record_etree.xpath(
            self.XPATHS[self.schema]["link_no_protocol"],
            namespaces=self.namespaces)

    def _update_links(self, uuid, new_link, link_type):
        """
        Base function for updating links
        """
        tree = self.record_etree
        self._get_links_from_record(uuid)
        record_links = self._current_link_urls()

        new_link_layer_name = None
        new_link_split = new_link.split(self.INNER_DELIMITER)
        new_link = new_link_split[0]
        if len(new_link_split) > 1:
            new_link_layer_name = new_link_split[1]

        #import pdb; pdb.set_trace()
        links_to_update = self._check_for_links_to_update(link_type)
        resources_no_protocol = self._get_resources_no_protocol()
        if len(links_to_update) == 0 and resources_no_protocol is not None:
            self._update_links_no_protocol(new_link,
                                           link_type,
                                           resources_no_protocol)

        # log.debug(etree.tostring(self.record_etree))
        for i in links_to_update:
            elem = i.find("gmd:linkage/gmd:URL", namespaces=self.namespaces)
            layer_name_elem = i.find(
                "gmd:name/gco:CharacterString", namespaces=self.namespaces)
            current_val = elem.text
            current_protocol = i.find(
                "gmd:protocol/gco:CharacterString", namespaces=self.namespaces)
            log.debug("Current protocol: {p}".format(p=current_protocol.text))
            log.debug("Current text: {t}".format(t=current_val))

            if (current_protocol.text in self.protocols_list and
                    current_protocol.text != "WWW:DOWNLOAD"):

                if "wms" in current_protocol.text.lower() and new_link_layer_name is not None and layer_name_elem is None:

                    name = etree.SubElement(i,
                                            "{ns}name".format(
                                                ns="{" + self.namespaces["gmd"] + "}"),
                                            nsmap=self.namespaces)
                    name_text = etree.SubElement(name,
                                                 "{ns}CharacterString".format(
                                                     ns="{" + self.namespaces["gco"] + "}"),
                                                 nsmap=self.namespaces)
                    name_text.text = new_link_layer_name
                    self.tree_changed = True

                if current_val and current_val == new_link:
                    # if so, we have nothing to do!
                    log.info(
                        "Value is already set to {link}. Skipping!".format(link=new_link))
                    continue

                else:
                    log.debug("Updating link from {old} to {new}".format(old=current_val,
                                                                         new=new_link))
                    elem.text = new_link
                    record_links.append(new_link)
                    record_links.remove(current_val)
                    self.tree_changed = True

                    xpath = self.record_etree.getpath(elem)
                    xpath = "/".join(xpath.split("/")[2:])

            else:
                log.debug("Updating protocol from {old} to {new}".format(old=current_protocol.text,
                                                                         new=self.protocols_list[0]))
                value = self.protocols_list[0]
                current_protocol = value
                self.tree_changed = True

                xpath = self.record_etree.getpath(current_protocol)
                xpath = "/".join(xpath.split("/")[2:])

        # if the new url is nowhere to be found, create a new resource
        if new_link not in record_links:

            log.debug("Current links: " + ", ".join(self._current_link_urls()))
            log.debug("Creating a new link")
            self._create_new_link(new_link, link_type)
            log.debug("Updated links: " + ", ".join(self._current_link_urls()))

    def _make_new_topic_element(self, cat_text):
        p = etree.Element("{gmd}topicCategory".format(
            gmd="{" + self.namespaces["gmd"] + "}"), nsmap=self.namespaces)
        c = etree.SubElement(p, "{gmd}MD_TopicCategoryCode".format(
            gmd="{" + self.namespaces["gmd"] + "}"))
        c.text = cat_text
        return p


#CITATION ELEMENTS

    def NEW_title(self, uuid, new_title):
        """
        Updates title of record
        """
        if new_title != "" and new_title != "SKIP":
            update = self._simple_element_update(
                uuid, new_title, element="title")
            log.info("updated title")

    def NEW_identifier(self, uuid, new_identifier):
        """
        Updates resource identifier of record
        """
        if new_identifier != "" and new_identifier != "SKIP":
            update = self._simple_element_update(
                uuid, new_identifier, element="identifier")
            log.info("updated identifier")


    def NEW_collective_title(self, uuid, new_collective_title):
        """
        Updates collection of record
        """
        if new_collective_title != "" and new_collective_title != "SKIP":
            update = self._simple_element_update(
                uuid, new_collective_title, element="collective_title")
            log.info("updated collection name")

    def NEW_abstract(self, uuid, new_abstract):
        """
        Updates abstract of record
        """
        if new_abstract != "" and new_abstract != "SKIP":
            update = self._simple_element_update(
                uuid, new_abstract, element="abstract")
            log.info("updated abstract")

    def NEW_other_citation(self, uuid, new_other_citation):
        """
        Updates geometry type of record for GeoBlacklight Metadata crosswalk
        """
        if new_other_citation != "" and new_other_citation != "SKIP":
            update = self._simple_element_update(
                uuid, new_other_citation, element="other_citation")
            log.info("updated other citation")

    def NEW_credit(self, uuid, new_credit):
        """
        Updates credit
        """
        if new_credit != "" and new_credit != "SKIP":
            update = self._simple_element_update(
                uuid, new_credit, element="credit")
            log.info("updated credit")

    def NEW_thumbnail(self, uuid, new_thumbnail):
        """
        Updates thumbnail
        """
        if new_thumbnail != "" and new_thumbnail != "SKIP":
            update = self._simple_element_update(
                uuid, new_thumbnail, element="thumbnail")
            log.info("updated thumbnail")


    #can update but doesn't create new element
    def NEW_parent(self, uuid, new_parent):
        """
        Updates parent ID
        """

        if new_parent != "" and new_parent != "SKIP":
            update = self._base_element_update(
				uuid, new_parent, element="parent")
            log.info("updated parent record")



#METADATA

    def NEW_maintenance_note(self, uuid, new_maintenance_note):
        """
        Updates metadata maintenance note of record
        """
        if new_maintenance_note != "" and new_maintenance_note != "SKIP":
            update = self._simple_element_update(
                uuid, new_maintenance_note, element="maintenance_note")
            log.info("updated metadata maintenance note")

    def NEW_provenance(self, uuid, new_provenance):
        """
        Updates provenance of record
        """
        if new_provenance != "" and new_provenance != "SKIP":
            update = self._simple_element_update(
                uuid, new_provenance, element="provenance")
            log.info("updated provenance")


##    doesn't work if not already present - says no change even if there is one
    def NEW_dataset_uri(self, uuid, new_dataset_uri):
        """
        Updates datasetURI of record
        """
        if new_dataset_uri != "" and new_dataset_uri != "SKIP":
            update = self._base_element_update(
                uuid, new_dataset_uri, element="dataset_uri")
            log.info("updated datasetURI")



#SPATIAL

    def NEW_ref_system(self, uuid, new_ref_system):
        """
        Updates reference system of record
        """
        if new_ref_system != "" and new_ref_system != "SKIP":
            update = self._simple_element_update(
                uuid, new_ref_system, element="ref_system")
            log.info("updated reference system")

    def NEW_north_extent(self, uuid, new_north_extent):
        """
        Updates north extent of record
        """
        if new_north_extent != "" and new_north_extent != "SKIP":
            update = self._simple_element_update(
                uuid, new_north_extent, element="north_extent")
            log.info("updated north extent")

    def NEW_south_extent(self, uuid, new_south_extent):
        """
        Updates south extent of record
        """
        if new_south_extent != "" and new_south_extent != "SKIP":
            update = self._simple_element_update(
                uuid, new_south_extent, element="south_extent")
            log.info("updated south extent")

    def NEW_east_extent(self, uuid, new_east_extent):
        """
        Updates east extent of record
        """
        if new_east_extent != "" and new_east_extent != "SKIP":
            update = self._simple_element_update(
                uuid, new_east_extent, element="east_extent")
            log.info("updated east extent")

    def NEW_west_extent(self, uuid, new_west_extent):
        """
        Updates west extent of record
        """
        if new_west_extent != "" and new_west_extent != "SKIP":
            update = self._simple_element_update(
                uuid, new_west_extent, element="west_extent")
            log.info("updated west extent")


#DISTRIBUTION

    def NEW_distribution_format(self, uuid, new_format):
        """
        Updates distribution format of record
        """
        if new_format != "" and new_format != "SKIP":
            update = self._simple_element_update(
                uuid, new_format, element="distribution_format")
            log.info("updated distribution format")

    def NEW_link_download(self, uuid, new_link):
        if new_link != "" and new_link != "SKIP":
            update = self._update_links(uuid, new_link, "download")
            log.info("updated download link")

    def NEW_link_service_esri(self, uuid, new_link):
        if new_link != "" and new_link != "SKIP":
            update = self._update_links(uuid, new_link, "esri_service")
            log.info("updated esri_service link")

    def NEW_link_service_wms(self, uuid, new_link):
        if new_link != "" and new_link != "SKIP":
            update = self._update_links(uuid, new_link, "wms_service")
            log.info("updated wms_service link")

    def NEW_link_information(self, uuid, new_link):
        if new_link != "" and new_link != "SKIP":
            update = self._update_links(uuid, new_link, "information")
            log.info("updated info link")

    def NEW_link_metadata(self, uuid, new_link):
        if new_link != "" and new_link != "SKIP":
            update = self._update_links(uuid, new_link, "metadata")
            log.info("updated metadata link")

    def _delete_link_elementset(self, link):
        onLine = link.getparent().getparent().getparent()
        p = onLine.getparent()
        p.remove(onLine)
        self.tree_changed = True

    def DELETE_link_no_protocol(self, uuid, link_to_delete):
        if link_to_delete != "":
            links = self._get_resources_no_protocol()
            for link in links:
                if link.findtext("gmd:linkage/gmd:URL", namespaces=self.namespaces) == link_to_delete:
                    self._delete_link_elementset(link)
                    log.info("deleted link with no protocol: {link}".format(
                        link=link_to_delete))

    def DELETE_link(self, uuid, link_to_delete):
        if link_to_delete != "":
            links = self._current_link_url_elements()
            for link in links:
                if link.text == link_to_delete:
                    self._delete_link_elementset(link)
                    log.info("deleted link: {link}".format(
                        link=link_to_delete))


#TOPICS AND KEYWORDS

    def NEW_topic_categories(self, uuid, new_topic_categories):
        """
        This is heinous. I'm sorry.
        """
        cat_list = new_topic_categories.split(self.INNER_DELIMITER)
        log.debug("NEW TOPIC INPUT: " + new_topic_categories)

        if len(cat_list) == 1 and cat_list[0] == "":
            return

        tree = self.record_etree
        tree_changed = False
        xpath = self.XPATHS[self.schema]["topic_categories"]
        existing_cats = tree.findall(xpath, namespaces=self.namespaces)
        existing_cats_text = [
            i.text for i in existing_cats if i.text is not None]
        log.debug("existing_cats_text: {e}".format(e=existing_cats_text))

        new_cats = list(set(cat_list) - set(existing_cats_text))
        delete_cats = list(set(existing_cats_text) - set(cat_list))

        log.debug("NEW CATEGORIES: " + ", ".join(new_cats))
        log.debug("CATEGORIES TO DELETE: " + ", ".join(delete_cats))

        for cat_text in new_cats:

            if cat_text not in self.topic_categories:
                log.warn("Invalid topic category not added: " + cat_text)
                continue

            new_cat_element = self._make_new_topic_element(cat_text)
            elem = None

            if len(existing_cats) > 0:
                elem = existing_cats[-1].getparent()
            else:
                md_di = tree.find(self.XPATHS[self.schema][
                                  "md_data_identification"], namespaces=self.namespaces)

                potential_siblings = [
                    "gmd:characterSet", "gmd:language",
                    "gmd:spatialResolution", "gmd:spatialResolutionType",
                    "gmd:aggregationInfo", "gmd:resourceConstraints",
                    "gmd:resourceSpecificUsage", "gmd:descriptiveKeywords",
                    "gmd:resourceFormat", "gmd:graphicOverview",
                    "gmd:resourceMaintenance", "gmd:pointOfContact",
                    "gmd:status", "gmd:credit", "gmd:purpose", "gmd:abstract"]

                for i in potential_siblings:
                    log.debug("Looking for: {e}".format(e=i))
                    elem = md_di.find(i, namespaces=self.namespaces)
                    if elem is not None:
                        log.debug("Found: {e}".format(e=i))
                        break

            elem.addnext(new_cat_element)
            self.tree_changed = True

        for delete_cat in delete_cats:
            del_ele = tree.xpath("//gmd:MD_TopicCategoryCode[text()='{cat}']".format(
                cat=delete_cat), namespaces=self.namespaces)
            if len(del_ele) == 1:
                p = del_ele[0].getparent()
                p.remove(del_ele[0])
                pp = p.getparent()
                pp.remove(p)
                self.tree_changed = True

    def NEW_keywords_place(self, uuid, new_keywords):
        update = self._multiple_element_update(
            uuid, new_keywords, "keywords_place")
        log.info("updated place keywords")

    def NEW_keywords_theme(self, uuid, new_keywords):
        update = self._multiple_element_update(
            uuid, new_keywords, "keywords_theme")
        log.info("updated theme keywords")

    def _make_new_descriptive_keywords(self, tree):

        if len(dk) > 0:
            print("returning dk")
            e = etree.Element(
                "{ns}descriptiveKeywords".format(
                    ns="{" + self.namespaces["gmd"] + "}"),
                nsmap=self.namespaces)
            dk[-1].addnext()
            return dk[-1].getnext()
        else:
            md_di = tree.find(self.XPATHS[self.schema][
                              "md_data_identification"], namespaces=self.namespaces)
            if md_di is not None:
                print("making new dk")
                return etree.SubElement(md_di, "{ns}descriptiveKeywords".format(ns="{" + self.namespaces["gmd"] + "}"), nsmap=self.namespaces)

    def _make_new_keyword_thesaurus_elements(self, thesaurus_name):
        tree = self.record_etree
        dk = tree.findall(
            self.XPATHS[self.schema]["descriptive_keywords"],
            namespaces=self.namespaces
        )
        thesaurus = self._parse_snippet(
            "thesaurus_{n}.xml".format(n=thesaurus_name)
        )
        elem = None

        if len(dk) > 0:
            elem = dk[-1]
        else:
            md_di = tree.find(
                self.XPATHS[self.schema]["md_data_identification"],
                namespaces=self.namespaces
            )

            # ATM I can't think of a better way to make sure a fresh desc kw
            # elem gets placed in the correct spot.
            potential_siblings = [
                "gmd:resourceFormat", "gmd:graphicOverview",
                "gmd:resourceMaintenance", "gmd:pointOfContact", "gmd:status",
                "gmd:credit", "gmd:purpose", "gmd:abstract"]

            for i in potential_siblings:
                log.debug("Looking for: {e}".format(e=i))
                elem = md_di.find(i, namespaces=self.namespaces)
                if elem is not None:
                    log.debug("Found: {e}".format(e=i))
                    break

        elem.addnext(thesaurus)
        return elem.getnext().find(
            "gmd:MD_Keywords",
            namespaces=self.namespaces
        )

    def _make_new_keyword_anchor(self, value, uri, parent_node):
        element = etree.Element(
            "{ns}keyword".format(ns="{" + self.namespaces["gmd"] + "}"),
            nsmap=self.namespaces
        )
        child_element = etree.SubElement(
            element,
            "{ns}Anchor".format(ns="{" + self.namespaces["gmx"] + "}")
        )
        child_element.set(
            "{" + self.namespaces["xlink"] + "}href",
            self.GEMET_ANCHOR_BASE_URI + uri
        )
        child_element.text = value
        parent_node.insert(0, element)

    def _make_new_keyword_text(self, value, parent_node):
        element = etree.Element(
            "{ns}keyword".format(ns="{" + self.namespaces["gmd"] + "}"),
            nsmap=self.namespaces
        )
        child_element = etree.SubElement(
            element,
            "{ns}CharacterString".format(ns="{" + self.namespaces["gco"] + "}"))
        child_element.text = value
        parent_node.insert(0, element)

    def _keywords_thesaurus_update(
            self,
            uuid,
            new_vals_string,
            ids=None,
            kw_type=None,
            thesaurus=None):
        """
        This is heinous. I'm sorry.
        """
        log.info("KEYWORD TYPE: {t}".format(t=kw_type))
        log.info("THESAURUS: {t}".format(t=thesaurus))
        log.info("NEW VALUE INPUT: " + new_vals_string)

        new_vals_list = new_vals_string.split(self.INNER_DELIMITER)

        if ids:
            new_ids_list = ids.split(self.INNER_DELIMITER)

            if len(new_ids_list) == 1 and new_ids_list[0] == "":
                return

        if len(new_vals_list) == 1 and new_vals_list[0] == "":
            return

        tree = self.record_etree

        thesaurus_xpath = self.XPATHS[self.schema][
            "keywords_{kw_type}_{thesaurus}".format(
                kw_type=kw_type,
                thesaurus=thesaurus)]
        existing_thesaurus = tree.xpath(
            thesaurus_xpath,
            namespaces=self.namespaces
        )

        if len(existing_thesaurus) == 0:
            md_kw = self._make_new_keyword_thesaurus_elements(thesaurus)

            if ids:
                for index, value in enumerate(new_vals_list):
                    self._make_new_keyword_anchor(
                        value, new_ids_list[index], md_kw)
                    self.tree_changed = True
            else:
                for value in new_vals_list:
                    self._make_new_keyword_text(value, md_kw)
                    self.tree_changed = True
        else:
            existing_vals = existing_thesaurus[0].getparent().getparent().getparent(
            ).getparent().findall("gmd:keyword/*", namespaces=self.namespaces)
            existing_vals_parent = existing_vals[0].getparent().getparent()
            existing_vals_list = [i.text for i in existing_vals]

            log.info("EXISTING VALUES: " + ", ".join(existing_vals_list))

            add_values = list(set(new_vals_list) - set(existing_vals_list))
            delete_values = list(set(existing_vals_list) - set(new_vals_list))

            # TODO can't handle going between anchor/text, but i don't
            # care!!!!! hahahahha
            if ids:
                # Are these even necessary? Not doing anything with em anyway
                existing_ids = [i.get("{{ns}}href".format(
                    ns="{" + self.namespaces["xlink"] + "}")) for i in existing_vals]
                add_ids = list(set(new_ids_list) - set(existing_ids))
                delete_ids = list(set(existing_ids) - set(new_ids_list))

            log.info("VALUES TO ADD: " + ", ".join(add_values))
            log.info("VALUES TO DELETE: " + ", ".join(delete_values))

            for delete_value in delete_values:
                # TODO abstract out keyword specifics
                del_ele = tree.xpath("//gmd:keyword/*[text()='{val}']".format(
                    val=delete_value), namespaces=self.namespaces)
                if len(del_ele) == 1:
                    p = del_ele[0].getparent()
                    p.remove(del_ele[0])
                    pp = p.getparent()
                    pp.remove(p)
                    self.tree_changed = True

            for value in add_values:

                if ids:
                    self._make_new_keyword_anchor(
                        value,
                        add_ids[index],
                        existing_vals_parent
                    )
                else:
                    self._make_new_keyword_text(value, existing_vals_parent)
                self.tree_changed = True

    def _keywords_theme_gemet_update(
            self,
            uuid,
            new_vals_string,
            new_ids_string):
        """
        This is heinous. I'm sorry.
        """
        log.info("NEW VALUE INPUT: " + new_vals_string)

        new_vals_list = new_vals_string.split(self.INNER_DELIMITER)
        new_ids_list = new_ids_string.split(self.INNER_DELIMITER)

        if len(new_vals_list) == 1 and new_vals_list[0] == "" or \
                len(new_ids_list) == 1 and new_ids_list[0] == "":
            return

        tree = self.record_etree
        thesaurus_xpath = self.XPATHS[self.schema]["keywords_theme_gemet"]
        existing_thesaurus = tree.xpath(
            thesaurus_xpath,
            namespaces=self.namespaces
        )

        if len(existing_thesaurus) == 0:
            md_kw = self._make_new_keyword_thesaurus_elements()
            self.tree_changed = True

            for index, value in enumerate(new_vals_list):
                self._make_new_keyword_anchor(
                    value,
                    new_ids_list[index],
                    md_kw
                )

        else:
            existing_vals = existing_thesaurus[0].getparent().getparent().getparent(
            ).getparent().findall("gmd:keyword/gmx:Anchor", namespaces=self.namespaces)
            existing_vals_parent = existing_vals[0].getparent().getparent()
            existing_vals_list = [i.text for i in existing_vals]
            existing_ids = [i.get("{{ns}}href".format(
                ns="{" + self.namespaces["xlink"] + "}")) for i in existing_vals]

            log.info("EXISTING VALUES: " + ", ".join(existing_vals_list))

            add_values = list(set(new_vals_list) - set(existing_vals_list))
            add_ids = list(set(new_ids_list) - set(existing_ids))
            delete_values = list(set(existing_vals_list) - set(new_vals_list))
            delete_ids = list(set(existing_ids) - set(new_ids_list))

            log.info("VALUES TO ADD: " + ", ".join(add_values))
            log.info("VALUES TO DELETE: " + ", ".join(delete_values))

            for delete_value in delete_values:
                # TODO abstract out keyword specifics
                del_ele = tree.xpath(
                    "//gmd:keyword/gmx:Anchor[text()='{val}']".format(
                        val=delete_value),
                    namespaces=self.namespaces)
                if len(del_ele) == 1:
                    p = del_ele[0].getparent()
                    p.remove(del_ele[0])
                    pp = p.getparent()
                    pp.remove(p)
                    self.tree_changed = True

            for value in add_values:
                self._make_new_keyword_anchor(
                    value,
                    add_ids[index],
                    existing_vals_parent
                )
                self.tree_changed = True

    def NEW_keywords_theme_gemet_name(self, uuid, gemet_names):
        gemet_ids = self.row["NEW_keywords_theme_gemet_id"]
        self._keywords_thesaurus_update(
            uuid,
            gemet_names,
            ids=gemet_ids,
            kw_type="theme",
            thesaurus="gemet"
        )
        log.info("updated gemet keywords")

    def NEW_keywords_place_geonames(self, uuid, geonames):
        self._keywords_thesaurus_update(
            uuid,
            geonames,
            kw_type="place",
            thesaurus="geonames"
        )
        log.info("updated geonames keywords")


#DATES

    def _date_or_datetime(self, date_element):
        e = date_element.find("gco:Date", namespaces=self.namespaces)
        if e is None:
            e = date_element.find("gco:Datetime", namespaces=self.namespaces)
        return e

    def _add_gcodate_to_date(self, date_elem, new_date):
        date_e = etree.SubElement(
            date_elem,
            "{ns}Date".format(ns="{" + self.namespaces["gco"] + "}"),
            nsmap=self.namespaces)
        date_e.text = new_date

    def _add_datetypecode_to_datetype(self, datetype, date_type):
        datetypecode = etree.SubElement(
            datetype,
            "{ns}CI_DateTypeCode".format(
                ns="{" + self.namespaces["gmd"] + "}"),
            nsmap=self.namespaces)
        datetypecode.set(
            "codeList",
            "http://www.isotc211.org/2005/resources/Codelist\
                /gmxCodelists.xml#CI_DateTypeCode")
        datetypecode.set("codeListValue", date_type)
        datetypecode.set("codeSpace", "002")
        datetypecode.text = date_type

    def _add_datetype_to_date(self, ci_date, date_type):
        datetype = etree.SubElement(
            ci_date,
            "{ns}dateType".format(ns="{" + self.namespaces["gmd"] + "}"),
            nsmap=self.namespaces)
        self._add_datetypecode_to_datetype(datetype, date_type)

    def _create_date(self, date_elem, new_date, date_type):
        log.info("Creating new {dt}.".format(dt=date_type))
        ci_date = etree.SubElement(
            date_elem,
            "{ns}CI_Date".format(ns="{" + self.namespaces["gmd"] + "}"),
            nsmap=self.namespaces)
        self._add_datetype_to_date(ci_date, date_type)

        date_elem2 = etree.SubElement(
            ci_date,
            "{ns}date".format(ns="{" + self.namespaces["gmd"] + "}"),
            nsmap=self.namespaces)
        self._add_gcodate_to_date(date_elem2, new_date)

    def _check_for_and_remove_nilreason(self, elem):
        nil_attrib = "{gco}nilReason".format(
            gco="{" + self.namespaces["gco"] + "}"
        )
        nil = elem.get(nil_attrib)
        if nil is not None:
            elem.attrib.pop(nil_attrib)
            self.tree_changed = True

    def _update_date(self, uuid, new_date, date_type):
        xpath = self.XPATHS[self.schema]["ci_date_type"].format(
            datetype=date_type
        )
        new_date_parsed = parser.parse(new_date)
        iso_date = new_date_parsed.isoformat()[:10]
        tree = self.record_etree
        date_element = tree.xpath(
            xpath,
            namespaces=self.namespaces
        )

        if len(date_element) >= 1:
            log.debug("Found date element with matching type in citation")
            self._check_for_and_remove_nilreason(date_element[0])
            date_type_elem = self._date_or_datetime(date_element[0])
            if date_type_elem is not None:
                if date_type_elem.tag.endswith("Date"):
                    if date_type_elem.text != iso_date:
                        date_type_elem.text = iso_date
                        self.tree_changed = True
                elif date_type_elem.tag.endswith("DateTime"):
                    if date_type_elem.text != new_date_parsed.isoformat() + "Z":
                        date_type_elem.text = new_date_parsed.isoformat() + "Z"
                        self.tree_changed = True
            else:
                log.debug("Did not find date type in date element. \
                           Creating gco:Date.")
                self._add_gcodate_to_date(date_element[0], iso_date)
                self.tree_changed = True
        else:
            log.debug("No date element matching type found, \
                       looking for Citation.")
            # look for ancestor date element
            citation_xpath = self.XPATHS[self.schema]["citation"]
            citation_element = tree.xpath(
                citation_xpath,
                namespaces=self.namespaces)

            if len(citation_element) >= 1:
                log.debug("Found Citation")
                date_elem = citation_element[0].find(
                    "gmd:date",
                    namespaces=self.namespaces)
                if date_elem is not None:
                    log.debug("Found date in citation")
                    self._check_for_and_remove_nilreason(date_elem)
                    date_children = date_elem.getchildren()

                    if len(date_children) == 0:
                        self._create_date(date_elem, iso_date, date_type)
                        self.tree_changed = True
                    else:
                        ci_date = date_elem.find("gmd:CI_Date",
                                                 namespaces=self.namespaces)

                        if ci_date is None:
                            log.debug("date is malformed.")
                            [date_elem.remove(i) for i in date_children]
                            self._create_date(date_elem, iso_date, date_type)
                            self.tree_changed = True
                        elif (ci_date.find(
                              "gmd:dateType",
                              namespaces=self.namespaces) is not None and
                              ci_date.find(
                            "gmd:dateType",
                            namespaces=self.namespaces
                        ).get("codeListValue") != date_type):
                            date_elem = etree.SubElement(
                                citation_element[0],
                                "{ns}date".format(
                                    ns="{" + self.namespaces["gmd"] + "}"
                                ),
                                nsmap=self.namespaces)
                            self._create_date(date_elem, iso_date, date_type)
                            self.tree_changed = True
                        elif ci_date.find("gmd:dateType",
                                          namespaces=self.namespaces) is None:
                            self._add_datetype_to_date(ci_date, date_type)
                else:
                    date_elem = etree.SubElement(
                        citation_element[0],
                        "{ns}date".format(
                            ns="{" + self.namespaces["gmd"] + "}"),
                        nsmap=self.namespaces)
                    self._create_date(date_elem, iso_date, date_type)
                    self.tree_changed = True

    def _parse_snippet(self, snippet_name, path_to_snippet="xml_snippets"):
        snippet_tree = etree.parse(os.path.join(path_to_snippet, snippet_name))
        snippet_root = snippet_tree.getroot()
        return snippet_root

    def _add_extent(self):
        existing_extent = self.record_etree.xpath(
            self.XPATHS[self.schema]["extent"],
            namespaces=self.namespaces)
        last_extent = existing_extent[-1]
        new_extent = etree.Element(
            "{ns}extent".format(ns="{" + self.namespaces["gmd"] + "}"),
            nsmap=self.namespaces)
        ex_extent = etree.SubElement(
            new_extent,
            "{ns}EX_Extent".format(ns="{" + self.namespaces["gmd"] + "}"),
            nsmap=self.namespaces)
        last_extent.addnext(new_extent)
        log.info("Added new extent.")
        return ex_extent

    def _add_temporal_extent(self, new_date, ex_extent, t_e_type):
        if t_e_type == "start" or t_e_type == "end":
            t_e_base = self._parse_snippet("extent_temporal_range.xml")
        else:
            t_e_base = self._parse_snippet("extent_temporal_instant.xml")
        ex_extent.append(t_e_base)
        log.debug("added temporal extent. recursing to set value")
        self.tree_changed = True
        self._update_temporal_extent(new_date, t_e_type)

    def _update_temporal_extent(self, new_date, temporal_extent_type):
        #only adds 4 digit year
        if new_date != "now":
            new_date_parsed = parser.parse(new_date,ignoretz=True)
            iso_date = new_date_parsed.isoformat()[:4]
        else:
            iso_date = "now"

        tree = self.record_etree

        xpath = self.XPATHS[self.schema]["temporalextent"]
        temporalextent = tree.xpath(
            xpath,
            namespaces=self.namespaces
        )

        if len(temporalextent) >= 1:
            te = temporalextent[0]
            xpath = self.XPATHS[self.schema]["temporalextent_" +
                                             temporal_extent_type]
            log.debug(xpath)
            te_elem = te.find(xpath, namespaces=self.namespaces)
            if te_elem is not None:
                te_elem.text = iso_date
                log.debug("found and updated temporal extent: {iso}".format(
                    iso=iso_date)
                )
                self.tree_changed = True
            else:
                p_ex_extent = te.getparent()
                p_ex_extent.remove(te)
                log.debug("adding temporal extent to existing extent")
                self._add_temporal_extent(
                    new_date,
                    p_ex_extent,
                    temporal_extent_type)
        else:
            ex_extent = self._add_extent()
            self._add_temporal_extent(
                new_date,
                ex_extent,
                temporal_extent_type)

    def NEW_date_publication(self, uuid, new_date):
        if new_date != "":
            update = self._update_date(uuid, new_date, "publication")
            log.info("updated publication date!")

    def NEW_date_revision(self, uuid, new_date):
        if new_date != "":
            update = self._update_date(uuid, new_date, "revision")
            log.info("updated revision date!")

    def NEW_temporal_start(self, uuid, new_date):
        if new_date != "":
            update = self._update_temporal_extent(new_date, "start")
            log.info("updated temporal start date!")

    def NEW_temporal_end(self, uuid, new_date):
        if new_date != "":
            update = self._update_temporal_extent(new_date, "end")
            log.info("updated temporal end date!")

    def NEW_temporal_instant(self, uuid, new_date):
        if new_date != "":
            update = self._update_temporal_extent(new_date, "instant")
            log.info("updated temporal instant date!")


#CONTACTS
	#these elements must be present, won't create new

    def NEW_publisher(self, uuid, new_publisher):
        """
        Updates publisher of record
        """
        if new_publisher != "" and new_publisher != "SKIP":
            update = self._simple_element_update(
                uuid, new_publisher, element="publisher")
            log.info("updated publisher")

    def NEW_originator(self, uuid, new_originator):
        """
        Updates originator of record
        """
        if new_originator != "" and new_originator != "SKIP":
            update = self._simple_element_update(
                uuid, new_originator, element="originator")
            log.info("updated originator")

    def NEW_distributor(self, uuid, new_distributor):
        """
        Updates distributor of record
        """
        if new_distributor != "" and new_distributor != "SKIP":
            update = self._simple_element_update(
                uuid, new_distributor, element="distributor")
            log.info("updated distributor")


#PROCESS


    def _get_etree_for_record(self, uuid):
        """
        Set self.record_etree to etree ElementTree
        of record with inputted uuid.
        """

        xml = self.records[uuid].xml
        root = etree.fromstring(xml)
        return etree.ElementTree(root)

    def get_record_by_id(self, uuid):
        """
        Requests a record via the provided uuid.
        Sets self.records[uuid] to the result. Returns nothing.
        """

        # if self.schema == "iso19139":
        outschema = "http://www.isotc211.org/2005/gmd"
        log.debug("get_record_by_id: requesting fresh XML.")
        self.csw.getrecordbyid(id=[str(uuid)], outputschema=outschema)
        time.sleep(1)
        if uuid in self.csw.records:
            log.debug("get_record_by_id: got the xml")
            self.records[uuid] = self.csw.records[uuid]


    def process_spreadsheet(self):
        """
        Iterates through the inputted CSV, detects NEW_ fields, and executes
        update functions as needed.
        """
        for row in self.reader:
            log.debug(row)
            self.row_changed = False
            self.tree_changed = False
            self.record_etree = False

            self.row = row
            if "uuid" in row:
                self.uuid = row["uuid"]
            elif "UUID" in row:
                self.uuid = row["UUID"]
            else:
                sys.exit("No uuid column found. Must be named uuid or UUID.")

            if self.uuid == "DELETED" or self.uuid == "SKIP":
                continue

            log.debug(self.uuid)

            self.schema = "iso19139"
            self.get_record_by_id(self.uuid)
            self.record_etree = self._get_etree_for_record(self.uuid)

            for field in self.fieldnames:
                if field.lower() == "uuid":
                    continue

                if field in self.field_handlers[self.schema] and field in row:
                    self.field_handlers[self.schema][field].__call__(
                        self.uuid,
                        row[field]
                    )

            if self.uuid in self.records and self.tree_changed:
                log.debug("replacing entire XML")
                new_xml = etree.tostring(self.record_etree)
                self.row_changed = True
                self.csw.transaction(
                    ttype="update",
                    typename='csw:Record',
                    record=new_xml,
                    identifier=self.uuid)
                time.sleep(2)
               #  self.update_timestamp(self.uuid)
                log.info("Updated: {uuid}\n\n".format(uuid=self.uuid))
            else:
                log.info("No change: {uuid}\n\n".format(uuid=self.uuid))


def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "input_csv",
        help="indicate path to the csv containing the updates")
    args = parser.parse_args()
    f = UpdateCSW(CSW_URL_PUB, CSW_USER, CSW_PASSWORD, args.input_csv)
    f.process_spreadsheet()

if __name__ == "__main__":
    sys.exit(main())
