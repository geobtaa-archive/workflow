<?php /* REQUIRES PHP 5.2 OR LATER, on account of json_encode() function. */ ?>
<?php
$entire_request_begin = microtime(true);
include 'location_DB.php';
include 'config.php';

$runningtotal = 0;

if ($log_b) {
	$log = fopen($speedlog, "a") or die("Unable to open file!");
	$begin_time = getdate();
	$begin_statement = "Beginning record export, on ".$begin_time['mon']."-".$begin_time['mday']."-".$begin_time['year'].", at ".$begin_time['hours'].":".$begin_time['minutes'].":".$begin_time['seconds']." UTC\n//////////////////////////////////////////////\n\n";
	fwrite($log, $begin_statement);
	$email_report = $begin_statement;
};

$itemSum = 0;
foreach(loop('items') as $num):
if ($num) {
	$itemSum = $itemSum + 1;
	};

endforeach;

$itemSumInternal = 0;
echo("{");
foreach(loop('items') as $item):


$begin_item_time = microtime(true);

if ($item) {
	$itemSumInternal = $itemSumInternal + 1;
	};

//added to get file URLs km 20180426
set_loop_records('files', $item->Files);
foreach (loop('files') as $file):
endforeach;





/* METADATA VARIABLES */

$item = get_current_record('item');
// $file = get_current_record($item->Files);

$identifier = metadata($item, array('Dublin Core', 'Identifier'), array('all'=>true, 'no_escape'=>true));
$title = metadata($item, array('Dublin Core', 'Title'), array('all'=>true, 'no_escape'=>true));
$description = metadata($item, array('Dublin Core', 'Description'), array('all'=>true, 'no_escape'=>true));
$rights = metadata($item, array('Dublin Core', 'Rights'), array('all'=>true, 'no_escape'=>true));
$provenance = metadata($item, array('Dublin Core', 'Provenance'), array('all'=>true, 'no_escape'=>true));
$references = metadata($item, array('Dublin Core', 'References'), array('all'=>true, 'no_escape'=>true));
$format = metadata($item, array('Dublin Core', 'Format'), array('all'=>true, 'no_escape'=>true));
$language = metadata($item, array('Dublin Core', 'Language'), array('all'=>true, 'no_escape'=>true));
$type = metadata($item, array('Dublin Core', 'Type'), array('all'=>true, 'no_escape'=>true));
$publisher = metadata($item, array('Dublin Core', 'Publisher'), array('all'=>true, 'no_escape'=>true));
$creator = metadata($item, array('Dublin Core', 'Creator'), array('all'=>true, 'no_escape'=>true));
$subject = metadata($item, array('Dublin Core', 'Subject'), array('all'=>true, 'no_escape'=>true));
$dateIssued = metadata($item, array('Dublin Core', 'Date Issued'), array('all'=>true, 'no_escape'=>true));
$temporalCoverage = metadata($item, array('Dublin Core', 'Temporal Coverage'), array('all'=>true, 'no_escape'=>true));
$spatialCoverage = metadata($item, array('Dublin Core', 'Spatial Coverage'), array('all'=>true, 'no_escape'=>true));
$source = metadata($item, array('Dublin Core', 'Source'), array('all'=>true, 'no_escape'=>true));
$relation = metadata($item, array('Dublin Core', 'Relation'), array('all'=>true, 'no_escape'=>true));
$isPartOf = metadata($item, array('Dublin Core', 'Is Part Of'), array('all'=>true, 'no_escape'=>true));
$UUID = metadata($item, array('Dublin Core', 'Identifier'), array('all'=>true, 'no_escape'=>true));
$layerID = metadata($item, array('GeoBlacklight', 'Layer ID'), array('all'=>true, 'no_escape'=>true));
$slug = metadata($item, array('GeoBlacklight', 'Slug'), array('all'=>true, 'no_escape'=>true));
$geomType = metadata($item, array('Dublin Core', 'Medium'), array('all'=>true, 'no_escape'=>true));
$layerModDate = metadata($item, array('GeoBlacklight', 'Layer Modified Date'), array('all'=>true, 'no_escape'=>true));
$BBox = metadata($item, array('Dublin Core', 'Coverage'), array('all'=>true, 'no_escape'=>true));
$centroid = metadata($item, array('GeoBlacklight', 'Centroid'), array('all'=>true, 'no_escape'=>true));
$solrGeom = metadata($item, array('GeoBlacklight', 'Solr Geometry'), array('all'=>true, 'no_escape'=>true));
$solrYear = metadata($item, array('GeoBlacklight', 'Solr Year'), array('all'=>true, 'no_escape'=>true));

/*additional variables for links km*/
// $thumbnail = metadata($file,'uri', array('all' => true, 'no_escape'=>true));
$thumbnail = metadata($item, array('GeoBlacklight', 'Thumbnail'), array('all'=>true, 'no_escape'=>true));
$download = metadata($item, array('GeoBlacklight', 'Download File'), array('all'=>true, 'no_escape'=>true));
$information = metadata($item, array('GeoBlacklight', 'Information Page'), array('all'=>true, 'no_escape'=>true));
$ndex = metadata($item, array('GeoBlacklight', 'Open Index Map'), array('all'=>true, 'no_escape'=>true));
$esrirest = metadata($item, array('GeoBlacklight', 'Esri Rest Service'), array('all'=>true, 'no_escape'=>true));
$webmapservice = metadata($item, array('GeoBlacklight', 'Web Map Service'), array('all'=>true, 'no_escape'=>true));
$fgdc = metadata($item, array('Dublin Core', 'Bibliographic Citation'), array('all'=>true, 'no_escape'=>true));


if (count($identifier) == 1) {
	$identifier = $identifier[0];
} elseif (count($identifier) == 0) {
	$identifier = "";
	};

if (count($title) == 1) {
	$title = $title[0];
} elseif (count($title) == 0) {
	$title = "MISSING TITLE";
	};

if (count($description) == 1) {
	$description = $description[0];
}

if (count($rights) == 1) {
	$rights = $rights[0];
} elseif (count($rights) == 0) {
	$rights = "Restricted";
	};

if (count($provenance) == 1) {
	$provenance = $provenance[0];
} elseif (count($provenance) == 0) {
	$provenance = "";
	};

if (count($source) == 1) {
	$source = $source[0];
} elseif (count($source) == 0) {
	$source = "";
	};

/*count for new variables km*/
if (count($information) == 1) {
	$information = $information[0];
} elseif (count($information) == 0) {
	$information = "";
	};

if (count($download) == 1) {
	$download = $download[0];
} elseif (count($download) == 0) {
	$download = "";
	};

if (count($index) == 1) {
	$index = $index[0];
} elseif (count($index) == 0) {
	$index = "";
	};

if (count($fgdc) == 1) {
	$fgdc = $fgdc[0];
} elseif (count($fgdc) == 0) {
	$fgdc = "";
	};

if (count($esrirest) == 1) {
	$esrirest = $esrirest[0];
} elseif (count($esrirest) == 0) {
	$esrirest = "";
	};

if (count($webmapservice) == 1) {
	$esrirest = $webmapservice[0];
} elseif (count($webmapservice) == 0) {
	$webmapservice = "";
	};



if (count($references) == 1) {
	$references = $references[0];
} elseif (count($references) == 0) {
	$references = "";
	};

if (count($format) == 1) {
	$format = $format[0];
}
if (count($type) == 1) {
	$type = $type[0];
}

// if (count($creator) == 1) {
// 	$creator = $creator[0];
// }

if (count($dateIssued) == 1) {
	$dateIssued = $dateIssued[0];
}

if (count($temporalCoverage) == 1) {
	$temporalCoverage = array($temporalCoverage[0]);
} elseif (count($temporalCoverage) == 0) {
	$temporalCoverage = array();
	};

if (count($spatialCoverage) == 1) {
	$spatialCoverage = array($spatialCoverage[0]);
} elseif (count($spatialCoverage) == 0) {
	$spatialCoverage = array();
	};

// if (strpos($temporalCoverage[0],'-')) {
// 	$dashpos = strpos($temporalCoverage[0],'-');
// 	$year1 = substr($temporalCoverage[0], 0, $dashpos);
// 	$year2 = substr($temporalCoverage[0], $dashpos+1, strlen($temporalCoverage[0]));
// 	$temporalCoverage = array($year1,$year2);
// }

if (count($isPartOf) == 1) {
	$isPartOf = array($isPartOf[0]);
}

// if (count($UUID) == 1) {
// 	$UUID = $UUID[0];
// } elseif (count($UUID) == 0) {
// 	$UUID = "/UUID/NEEDED";
// 	};

if (count($layerID) == 1) {
	$layerID = $layerID[0];
} elseif (count($layerID) == 0) {
	$layerID = "";
	};

// if (count($thumbnail) == 1) {
// 	$thumbnail = $thumbnail[0];
// } elseif (count($thumbnail) == 0) {
// 	$thumbnail = "";
// 	};

if (count($slug) == 1) {
	$slug = $slug[0];
} elseif (count($slug) == 0) {
	$slug = "";
	};

if (count($geomType) == 1) {
	$geomType = $geomType[0];
} elseif (count($geomType) == 0) {
	$geomType = "";
	};

if (count($layerModDate) == 1) {
	$layerModDate = $layerModDate[0];
} elseif (count($layerModDate) == 0) {
	$layerModDate = "";
	};

if (count($BBox) !== 1) {
	$BBox = "0,0,0,0";
} elseif (count($BBox) == 1) {
	$BBox = $BBox[0];
	};

if (count($centroid) == 1) {
	$centroid = $centroid[0];
} elseif (count($centroid) == 0) {
	$centroid = "0,0,0,0";
	};

if (count($solrGeom) == 1) {
	$solrGeom = $solrGeom[0];
} elseif (count($solrGeom) == 0) {
	$solrGeom = "";
	};

if (count($solrYear) == 1) {
	$solrYear = $solrYear[0];
} elseif (count($solrYear) == 0) {
	$solrYear = $temporalCoverage[0];
	};

/*changed from geoserver to UUID km*/
if (is_array($layerID)) {
	if ($layerID[0] == "OVERRIDE") {
		$layerID = $layerID[1];
		};
} else {
	$layerID = strtolower($layerID);
};



// if (is_array($identifier)) {
// 	if ($identifier[0] == "OVERRIDE") {
// 	$identifier = $identifier[1];
// 	};
// } else {
// 	$identifier = $UUID;
// };

// /* relations/geonames suggest logic */
$geoIDstack = array();
$subAddStack = array();

$numPlace = count($spatialCoverage);


for ($x = 0; $x < $numPlace; $x++) {
    $Place = $spatialCoverage[$x];

    $PlaceColonPos = strpos($Place, ":");
    $geonameID = substr($Place, 0, $PlaceColonPos);
    array_push($geoIDstack, $geonameID);

    $placenamelen = strlen($Place) - $PlaceColonPos - 2;
    $placenameorig = substr($Place, $PlaceColonPos+2, $placenamelen);

    $PlacePar1 = strpos($Place, "(");
    $PlacePar2 = strpos($Place, ")");
    $paren = substr($Place, $PlacePar1 - 1, ($PlacePar2 - $PlacePar1 + 2));
    $placenametrim = str_replace($paren, '', $placenameorig);

    $comma1 = strpos($placenametrim, ",");
    $loc1 = substr($placenametrim, 0, $comma1);
    $comma2 = strpos($placenametrim, ",", $comma1 + 1);
    $comma3 = false;
    $loc2 = false;
    $loc3 = false;
    	if ($comma2 !== false) {
    		$loc2 = substr($placenametrim, $comma1 + 2, ($comma2 - $comma1) - 2);
    		$loc3 = substr($placenametrim, $comma2 + 2, strlen($placenametrim));
    	} else {
    		$loc2 = substr($placenametrim, $comma1 + 2, strlen($placenametrim));
    		$loc3 = false;
    	}
    if ($loc1 == "United States of America" || $loc1 == "United States") {
    	$printsub = "United States of America";
    } elseif (($loc1 == $loc2) && $loc3 == false) {
    	$printsub = $loc1;
    }  elseif (($loc1 == $loc2) && $loc3 !== false && $paren == " (country, state, region,...)") {
    	$printsub = $loc1.", ".$loc3;
    } elseif ($loc2 == false) {
    	$printsub = $loc1;
    } else {
    	$printsub = $loc1;
    		if ($loc2 !== false) {
    		$printsub = $printsub.", ".$loc2;
    			if ($loc3 !== false) {
    			$printsub = $printsub.", ".$loc3;
    			}
    		}
    	}


		array_push($subAddStack, $printsub);
};
// /* end rewrite */
//
// $spatialCoverage = array();
// $numSubAddStack = count($subAddStack);
//
// for ($x = 0; $x < $numSubAddStack; $x++) {
//     array_push($spatialCoverage, $subAddStack[$x]);
//
// }
//
// $relation = array();
//
// for ($x = 0; $x < $numPlace; $x++) {
//     $link = "http://sws.geonames.org/".$geoIDstack[$x]."";
//     array_push($relation, $link);
// }
//
// /*GeoNames API query for BBOX lookup */
// $geoIDnum = count($geoIDstack);
//
// $res_north = 0;
//
// if ($geoIDnum >= 1) {
//     $loclookup = $geoIDstack[0];
//     $query = array(
//         "geonameId" => $loclookup,
//         "username" => "majew030",
//     );
//
//     $cc_1 = curl_init();
//
//     curl_setopt($cc_1, CURLOPT_URL, "http://api.geonames.org/getJSON?" . http_build_query($query));
//     curl_setopt($cc_1, CURLOPT_HEADER, 0);
//     curl_setopt($cc_1, CURLOPT_RETURNTRANSFER, true);
//     $output = curl_exec($cc_1);
//     curl_close($cc_1);
//     $output_json = json_decode($output, $assoc = true);
//     if (array_key_exists('bbox', $output_json)) {
//     $res_north = $output_json['bbox']['north'];
//     $res_south = $output_json['bbox']['south'];
//     $res_east = $output_json['bbox']['east'];
//     $res_west = $output_json['bbox']['west'];
//     }
// };
//
// /*end geonames */


/*NEW REFERENCES ENCODING PICK ONE BY UNCOMMENTING*/

 /* information link only*/
//$references = "{\"http://schema.org/url\":\"".$information."\"}";


/* information, download*/
$references = "{\"http://schema.org/url\":\"".$information."\",\"http://schema.org/downloadUrl\":\"".$download."\"}";

 /* information, download,iiif*/
//$references = "{\"http://schema.org/url\":\"".$information."\",\"http://schema.org/downloadUrl\":\"".$download."\",\"http://iiif.io/api/image\":\"".$esrirest."\"}";


 /* information, esrimaplayer, download*/
//$references = "{\"http://schema.org/url\":\"".$information."\",\"urn:x-esri:serviceType:ArcGIS#DynamicMapLayer\":\"".$esrirest."\",\"http://schema.org/downloadUrl\":\"".$download."\"}";

 /* information, esrifeaturelayer, download*/
//$references = "{\"http://schema.org/url\":\"".$information."\",\"urn:x-esri:serviceType:ArcGIS#FeatureLayer\":\"".$esrirest."\",\"http://schema.org/downloadUrl\":\"".$download."\"}";

/* information, esriimagelayer*/
//$references = "{\"http://schema.org/url\":\"".$information."\",\"urn:x-esri:serviceType:ArcGIS#ImageMapLayer\":\"".$esrirest."\"}";


/* information, download, FGDC*/
//$references = "{\"http://schema.org/url\":\"".$information."\",\"http://schema.org/downloadUrl\":\"".$download."\",\"http://www.opengis.net/cat/csw/csdgm\":\"".$fgdc."\"}";

 /* information, esrimaplayer, download, fgdc*/
//$references = "{\"http://schema.org/url\":\"".$information."\",\"urn:x-esri:serviceType:ArcGIS#DynamicMapLayer\":\"".$esrirest."\",\"http://schema.org/downloadUrl\":\"".$download."\",\"http://www.opengis.net/cat/csw/csdgm\":\"".$fgdc."\"}";

 /* information, esrifeaturelayer, download, fgdc*/
//$references = "{\"http://schema.org/url\":\"".$information."\",\"urn:x-esri:serviceType:ArcGIS#FeatureLayer\":\"".$esrirest."\",\"http://schema.org/downloadUrl\":\"".$download."\",\"http://www.opengis.net/cat/csw/csdgm\":\"".$fgdc."\"}";



/* information, FGDC*/
//$references = "{\"http://schema.org/url\":\"".$information."\",\"http://www.opengis.net/cat/csw/csdgm\":\"".$fgdc."\"}";

/* information, ISO*/
//$references = "{\"http://schema.org/url\":\"".$information."\",\"http://www.isotc211.org/schemas/2005/gmd/\":\"".$fgdc."\"}";



/* polygon parser logic */

/* sample: -125.5339570045,32.7232795799,-113.9665679932,37.6842844962 as W S E N */

$numlocs = count($locDB);

// if (isset($spatialCoverage[0]) && $spatialCoverage[0] == "United States of America") {
//  	$BBox = "-170.1769013405,24.7073204053,-64.5665435791,71.6032483233";
//  } elseif (isset($spatialCoverage[0]) && ($spatialCoverage[0] == "World" || $spatialCoverage[0] == "Earth")) {
//  	$BBox = "-180,-90,180,90";
//  }

$flag = false;
if ($BBox !== "0,0,0,0") {

    $posCom1 = strpos($BBox, ",");
    $posCom2 = strpos($BBox, ",", $posCom1 + 1);
    $posCom3 = strpos($BBox, ",", $posCom2 + 1);

    $Slen = $posCom2 - $posCom1 - 1;
    $Elen = $posCom3 - $posCom2 - 1;
    $Nlen = strlen($BBox) - $posCom3 - 1;

    $W = substr($BBox, 0, $posCom1);
    $S = substr($BBox, $posCom1 + 1, $Slen);
    $E = substr($BBox, $posCom2 + 1, $Elen);
    $N = substr($BBox, $posCom3 + 1, $Nlen);
    $flag = true;
} else {
    $N = 0;
    $S = 0;
    $E = 0;
    $W = 0;
}

if ($res_north !== 0 && $flag == false) {
    $N = $res_north;
    $S = $res_south;
    $E = $res_east;
    $W = $res_west;
} else {};


$BBox = $S." ".$W." ".$N." ".$E;
$solrGeom = "ENVELOPE(".$W.", ".$E.", ".$N.", ".$S.")";
$centroidLat = ($N+$S)/2;
$centroidLong = ($E+$W)/2;
$centroid = $centroidLat.",".$centroidLong;


/* date parsing */

// $CDT = getdate();
$CDT = date("Y-m-d")."T".date("H:i:s")."Z";
// $layerModDate = $CDT['year']."-".$CDT['mon']."-".$CDT['mday']."T".$CDT['hours'].":".$CDT['minutes'].":".$CDT['seconds']."Z";
$layerModDate = $CDT;
?>
<?php echo(json_encode("output ".strval($itemSumInternal)));?>: [
{
"uuid": <?php echo(json_encode($identifier)); ?>,
"dc_identifier_s": <?php echo(json_encode($identifier)); ?>,
"dc_title_s": <?php echo(json_encode($title)); ?>,
"dc_description_s": <?php echo(json_encode($description)); ?>,
"dc_rights_s": <?php echo(json_encode($rights)); ?>,
"dc_format_s": <?php echo(json_encode($format)); ?>,
"dc_language_sm": <?php echo(json_encode($language)); ?>,
"dc_type_s": <?php echo(json_encode($type)); ?>,
"dc_publisher_sm": <?php echo(json_encode($publisher)); ?>,
"dc_creator_sm": <?php echo(json_encode($creator)); ?>,
"dc_subject_sm": <?php echo(json_encode($subject)); ?>,
"dct_provenance_s": <?php echo(json_encode($provenance)); ?>,
"dct_references_s": <?php echo(json_encode($references, JSON_UNESCAPED_SLASHES)); ?>,
"dct_isPartOf_sm": <?php echo(json_encode($isPartOf)); ?>,
"dct_issued_s": <?php echo(json_encode($dateIssued)); ?>,
"dct_temporal_sm": <?php echo(json_encode($temporalCoverage)); ?>,
"dct_spatial_sm": <?php echo(json_encode($spatialCoverage)); ?>,
"layer_slug_s": <?php echo(json_encode($identifier)); ?>,
"layer_geom_type_s": <?php echo(json_encode($geomType)); ?>,
"layer_modified_dt": <?php echo(json_encode($layerModDate)); ?>,
"solr_geom": <?php echo(json_encode($solrGeom)); ?>,
"solr_year_i": <?php echo(json_encode(intval($solrYear))); ?>,
"centroid_s": <?php echo(json_encode($centroid)); ?>,
"b1g_geonames_sm": <?php echo(json_encode($relation)); ?>,
"b1g_collection_sm": ["01d-01"],
"b1g_admin_note_sm": ["01d-01.03"],
"thumbnail_path_ss": <?php echo(json_encode($thumbnail)); ?>,
"geoblacklight_version": "1.0"
}

<?php if ($itemSumInternal < $itemSum) { echo("],"); } else { echo("] \n }"); };?>

<?php
if ($log_b) {
	$end_item_time = microtime(true);
	$item_time = $end_item_time - $begin_item_time;
	$runningtotal = $runningtotal + $item_time;
	$printed_item_time = "Item ".strval($itemSumInternal)."~~ Total processing time (ms): ".($item_time * 1000)."\n\n";
	fwrite($log, $printed_search_time);
	fwrite($log, $printed_item_time);
	$email_report = $email_report.$printed_item_time;
}

endforeach;

if ($log_b) {
	$entire_request_end = microtime(true);
	$entire_request_time = $entire_request_end - $entire_request_begin;
	$runTot_entire_diff = $entire_request_time - $runningtotal;
	$printed_request_time = "===============================================\nEntire query time: ".$entire_request_time." seconds, for ".strval($itemSumInternal)." items\nRunning total was 		".$runningtotal." seconds, with difference equaling ".($runTot_entire_diff*1000)." ms\n===============================================\n\n";
	fwrite($log, $printed_request_time);
	fclose($log);
//mail report
	if ($email_b) {
		$email_report = $email_report.$printed_request_time;
		$headers = 'From: ' . $from_email . "\r\n" .
    	'Reply-To: me@me.net' . "\r\n" .
    	'X-Mailer: PHP/' . phpversion();
		mail($to_email, 'Export Report', $email_report, $headers);
		}

}
?>



