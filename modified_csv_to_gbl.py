import csv
import json
import os
from datetime import datetime

single_dict = { #dictionary to translate single-value Dublin Core/GBL fields into GBLJson
    "Identifier":["dc_identifier_s"],
    "Slug":["layer_slug_s"],
    "Status":["b1g_status_s"],
    "Code":["b1g_code_s"],
    "Date Accessioned":["b1g_dateAccessioned_s"],
    "Date Retired":["b1g_dateRetired_s"],
    "Suppressed":["suppressed_b"],
    "Accrual Method":["dct_accrualMethod_s"],
    "Title":["dc_title_s"],
    "Description":["dc_description_s"],
    "Date Issued":["dct_issued_s"],
    "Solr Year":["solr_year_i"],
    "Provenance":["dct_provenance_s"],
    "Format":["dc_format_s"],
    "Geometry Type":["layer_geom_type_s"],
    "Image":["b1g_image_ss"],
    "Access":["b1g_access_s"],
    "Child":["b1g_child_record_b"],
    "Layer ID":["layer_id_s"],
    "Rights":["dc_rights_s"],
    "dct_references_s":["dct_references_s"],
    "b1g_centroid_ss": ["b1g_centroid_ss"],
    "solr_geom": ["solr_geom"]

    }
multiple_dict = { #dictionary to translate multivalue Dublin Core/GBL fields into GBLJson
    "Is Part Of":["dct_isPartOf_sm"],
    "Alternative Title":["dct_alternativeTitle_sm"],
    "Genre":["b1g_genre_sm"],
    "Subject":["dc_subject_sm"],
    "Keyword":["b1g_keyword_sm"],
    "Temporal Coverage":["dct_temporal_sm"],
    "Spatial Coverage":["dct_spatial_sm"],
    "Publisher":["dc_publisher_sm"],
    "Creator":["dc_creator_sm"],
    "Language":["dc_language_sm"],
    "Type":["dc_type_sm"],
    "Mediator":["dct_mediator_sm"],
    "Geonames":["b1g_geonames_sm"],
    "Date Range":["b1g_date_range_drsim"]
    }
if not os.path.exists("json"): #create a folder to store the jsons if one does not already exist
    os.mkdir("json")

csvfile = open('wilid.csv', 'r') #opens the csv with the GBL data. Change this as needed

reader = csv.DictReader(csvfile)
date_modified = datetime.today().strftime('%Y-%m-%d')+"T"+datetime.today().strftime('%X')+"Z" #sets date modified to the current date

for row in reader: #each row is a dictionary
    code = ""
    ref = []
    small_dict = {"geoblacklight_version":"1.0","layer_modified_dt":date_modified} #starting dictionary with set values
    for key,val in row.items():
        if key == "Code":
            code = val
            if not os.path.exists("json/" + val): #makes a new folder for each code
                os.mkdir("json/" + val)
        if key in single_dict:
            for fieldname in single_dict[key]:
                small_dict[fieldname] = val
        if key in multiple_dict:
            for fieldname in multiple_dict[key]:
                small_dict[fieldname] = val.split('|') #creates a list with the multiple values
    iden = row['Slug']
    filename = iden + ".json"
    with open("json/"+code+"/"+filename, 'w') as jsonfile: #writes to a json with the identifier as the filename
        json.dump(small_dict,jsonfile,indent=2)
