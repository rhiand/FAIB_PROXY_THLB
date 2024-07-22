Hailey Eckstrand
June 5, 2024

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