import csv
import json
import os
from datetime import datetime

single_dict = { #dictionary to translate single-value Dublin Core/GBL fields into GBLJson
    "dc_identifier_s":["layer_slug_s","dc_identifier_s"],
    "b1g_status_s":["b1g_status_s"],
    "b1g_code_s":["b1g_code_s"],
    "b1g_dateAccessioned_s":["b1g_dateAccessioned_s"],
    "b1g_dateRetired_s":["b1g_dateRetired_s"],
    "suppressed_b":["suppressed_b"],
    "dct_accrualMethod_s":["dct_accrualMethod_s"],
    "dc_title_s":["dc_title_s"],
    "dc_description_s":["dc_description_s"],
    "dct_issued_s":["dct_issued_s"],
    "solr_year_i":["solr_year_i"],
    "dct_provenance_s":["dct_provenance_s"],
    "dc_format_s":["dc_format_s"],
    "layer_geom_type_s":["layer_geom_type_s"],
    "b1g_image_ss":["b1g_image_ss"],
    "b1g_access_s":["b1g_access_s"],
    "layer_id_s":["layer_id_s"],
    "dct_references_s":["dct_references_s"],
    "dc_rights_s":["dc_rights_s"],
    "b1g_centroid_ss": ["b1g_centroid_ss"],
    "solr_geom": ["solr_geom"]
    }
multiple_dict = { #dictionary to translate multivalue Dublin Core/GBL fields into GBLJson
    "dct_isPartOf_sm":["dct_isPartOf_sm"],
    "dct_alternativeTitle_sm":["dct_alternativeTitle_sm"],
    "b1g_genre_sm":["b1g_genre_sm"],
    "dc_subject_sm":["dc_subject_sm"],
    "b1g_keyword_sm":["b1g_keyword_sm"],
    "dct_temporal_sm":["dct_temporal_sm"],
    "dct_spatial_sm":["dct_spatial_sm"],
    "dc_publisher_sm":["dc_publisher_sm"],
    "dc_creator_sm":["dc_creator_sm"],
    "dc_language_sm":["dc_language_sm"],
    "dc_type_sm":["dc_type_sm"],
    "dct_mediator_sm":["dct_mediator_sm"],
    "b1g_date_range_drsim":["b1g_date_range_drsim"]
    }
if not os.path.exists("json"): #create a folder to store the jsons if one does not already exist
    os.mkdir("json")

csvfile = open('ames.csv', 'r') #opens the csv with the GBL data. Change this as needed

reader = csv.DictReader(csvfile)
date_modified = datetime.today().strftime('%Y-%m-%d')+"T"+datetime.today().strftime('%X')+"Z" #sets date modified to the current date

for row in reader: #each row is a dictionary
    code = ""
    ref = []
    small_dict = {"geoblacklight_version":"1.0","layer_modified_dt":date_modified} #starting dictionary with set values
    for key,val in row.items():
        if key == "b1g_code_s":
            code = val
            if not os.path.exists("json/" + val): #makes a new folder for each code
                os.mkdir("json/" + val)
        if key in single_dict:
            for fieldname in single_dict[key]:
                small_dict[fieldname] = val
        if key in multiple_dict:
            for fieldname in multiple_dict[key]:
                small_dict[fieldname] = val.split('|') #creates a list with the multiple values

    iden = row['dc_identifier_s']
    filename = iden + ".json"
    with open("json/"+code+"/"+filename, 'w') as jsonfile: #writes to a json with the identifier as the filename
        json.dump(small_dict,jsonfile,indent=2)
