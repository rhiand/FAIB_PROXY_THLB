Hailey Eckstrand
June 5, 2024

BC Cumulative Effects Framework - Human Disturbance - 2023
Source -> https://catalogue.data.gov.bc.ca/dataset/7d61ff12-b85f-4aeb-ac8b-7b10e84b046c

ISTONE II All marks as of March 29 2016.xlsx
CSTONE II All marks as of March 8 2016.xlsx
Source -> G:\!Transfr\Stone_queries\2016\March 19, 2016
InteriorStoneQuery2016.csv
Source -> The first sheet of "ISTONE II All marks as of March 29 2016.xlsx", with the first 9 rows removed and saved as a csv.
CoastStoneQuery2016.csv
Source -> The first sheet of "CSTONE II All marks as of March 8 2016.xlsx", with the first 9 rows removed and saved as a csv.


InteriorStoneQuery2023.csv
Source -> G:\!Transfr\Stone_queries\2023\InteriorStoneQuery.csv
CoastStoneQuery2023.csv
Source -> G:\!Transfr\Stone_queries\2023\CoastStoneQuery.csv

BCTS_StreamData.gdb.zip
BCTS_StreamData.gpkg
Source -> From Mike Fowler: "There is a geopackage callled: BCTS_Field_Streams that is my attempt to extract the field surveyed streams from the BCTS stream inventory.  The zip file in the same folder is the BCTS source dataset in it's entirety.  The CLASS field is sadly not domain controlled and has (S6), S6, S6-A etc for values.  I will let you do the string massage magic to better consolidate the stream classes!"

bc_01ha_gr_skey.tif
Source -> S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif

BC_Boundary_Terrestrial.tif
Source -> S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\BC_Boundary_Terrestrial.tif

June13_Riparian data_for_FAIB.xlsx
Source -> email from Lisa Nordin, June, 2024

fish_passage_lines.gpkg.zip
fish_passage_lines.gpkg
fwa_stream_networks_channel_width.csv.gz
fwa_stream_networks_channel_width.csv
Source -> data shared via Simon Norris with approval by Craig Mount 
Data is only usable provided the resultant stream classification linework is never used for operational use. 
Do not share this data
Email chain: 
Subject: BC Fish Passage dataset
Date: June 4, 2024
From: Simon Norris via snorris@hillcrestgeo.ca
Email contents:
Hi Hailey, 

I heard that you'd moved on - congrats on the new gig, condolences on being back to working with fish passage data :)

As Craig probably mentioned, for the last year or so all focus has been publication of the salmon/steelhead models - there is no current Provincial model for 'all fish'.

The older 'all fish' model can be download here:
https://www.hillcrestgeo.ca/outgoing/fishpassage/data/FPTWG/v2.3.1/
username bcfishpassage
password restoration

Notes:
- this data predates all work/changes/fixes done in collaboration with CWF and I do not have a comprehensive list of what is missing/changed
- do not use the modelled crossings layer, the current version is in the latest distribution at https://catalogue.data.gov.bc.ca/dataset/freshwater-fish-habitat-accessibility-model-pacific-salmon-and-steelhead
- the files do not contain modelled channel width but you can grab it here: https://www.hillcrestgeo.ca/outgoing/public/fwapg/fwa_stream_networks_channel_width.csv.gz

Note the channel width model is not valid in areas with contributing areas outside of BC (and these streams are not noted in the data!).  Adding contributing areas outside of BC is still on my todo list but only PSF has been asking for this.

I'm not sure if the 'all fish' dataset contains gradient - if not I'll have to leave that one up to you. As you'll know, it is just a derived column that you can add like this: 
https://github.com/smnorris/fwapg/blob/main/db/tables.sql#L396C3-L397C70

If you can wait until I'm under contract with Craig again (or if MoF would like to support our work directly), it would be very straightforward to create and dump a current 'all fish' model with all the data you're looking for. 

hope this helps!

Simon


To preserve space, file's will be deleted after analysis. 