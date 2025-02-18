library(dadmtools)
source('src/utils/functions.R')

conn_list <- dadmtools::get_pg_conn_list()

query <- "DROP TABLE IF EXISTS thlb_proxy.vri_species_cd_datadict;"
run_sql_r(query, conn_list)

## create a table in postgres of all species and species full_name in the VRI species_cd_1 field
query <- 'CREATE TABLE thlb_proxy.vri_species_cd_datadict (
	species_cd varchar(10),
	species_full_name text
);'
run_sql_r(query, conn_list)

query <- "INSERT INTO thlb_proxy.vri_species_cd_datadict VALUES
('AC','Poplar'),
('ACT','Black Cottonwood'),
('AT','Trembling Aspen'),
('AX','Poplar Hybrid'),
('BA','Amabilis Fir'),
('BG','Grand Fir'),
('BL','Sub Alpine Fir'),
('BP','Noble Fir'),
('CW','Western Red Cedar'),
('DG','Green/Sitka Alder (N)'),
('DR','Red Alder'),
('EA','Alaska Paper Birch'),
('EP','Paper Birch'),
('FDC','Douglas-Fir (Coast)'),
('FDI','Douglas Fir (Interior)'),
('HM','Mountain Hemlock'),
('HW','Western Hemlock'),
('LA','Alpine Larch'),
('LARIDEC','European larch/Larix decidua'),
('LARIKAE','Japanese larch/Larix kaempferi'),
('LD','Dahurian Larch'),
('LS','Siberian Larch'),
('LT','Tamarack'),
('LW','Western Larch'),
('MB','Bigleaf Maple'),
('PA','Whitebark Pine'),
('PF','Limber Pine'),
('PICEABI','Norway spruce/Picea abies  '),
('PINUSYL','Scotch pine/Pinus sylvestris'),
('PJ','Jack Pine'),
('PLC','Lodgepole Pine (Coast)'),
('PLI','Lodgepole Pine (Interior)'),
('PW','Western White Pine'),
('PY','Yellow Pine'),
('SB','Black Spruce'),
('SN','Norway Spruce'),
('SS','Sitka Spruce'),
('SX','Spruce Hybrid'),
('SXS','Sitka X/Unknown Hybrid'),
('YC','Yellow Cypress');"
run_sql_r(query, conn_list)

query <- "INSERT INTO thlb_proxy.vri_species_cd_datadict (species_cd)
WITH all_species_1 AS (
	select
		species_cd_1 as species_cd
	FROM
		whse.veg_comp_lyr_r1_poly_internal_2023
	group by 
		species_cd_1
)
SELECT
	new_species.species_cd
from
all_species_1 new_species
LEFT JOIN thlb_proxy.vri_species_cd_datadict known_species USING (species_cd)
WHERE known_species.species_cd IS NULL AND new_species.species_cd is not null"
run_sql_r(query, conn_list)

## update species full name manually 
query <- "UPDATE thlb_proxy.vri_species_cd_datadict SET species_full_name = 'Larch' WHERE species_cd = 'L';"
run_sql_r(query, conn_list)
query <- "UPDATE thlb_proxy.vri_species_cd_datadict SET species_full_name = 'True Fir' WHERE species_cd = 'B';"
run_sql_r(query, conn_list)
query <- "UPDATE thlb_proxy.vri_species_cd_datadict SET species_full_name = 'White Spruce' WHERE species_cd = 'SW';"
run_sql_r(query, conn_list)
query <- "UPDATE thlb_proxy.vri_species_cd_datadict SET species_full_name = 'Other tree, not on list' WHERE species_cd = 'Z';"
run_sql_r(query, conn_list)
query <- "UPDATE thlb_proxy.vri_species_cd_datadict SET species_full_name = 'Pine' WHERE species_cd = 'P';"
run_sql_r(query, conn_list)
query <- "UPDATE thlb_proxy.vri_species_cd_datadict SET species_full_name = 'Hemlocks' WHERE species_cd = 'H';"
run_sql_r(query, conn_list)
query <- "UPDATE thlb_proxy.vri_species_cd_datadict SET species_full_name = 'Shasta Red Fir' WHERE species_cd = 'BM';"
run_sql_r(query, conn_list)
query <- "UPDATE thlb_proxy.vri_species_cd_datadict SET species_full_name = 'Spruce Hybrid' WHERE species_cd = 'SXE';"
run_sql_r(query, conn_list)
query <- "UPDATE thlb_proxy.vri_species_cd_datadict SET species_full_name = 'Arbutus' WHERE species_cd = 'RA';"
run_sql_r(query, conn_list)
query <- "UPDATE thlb_proxy.vri_species_cd_datadict SET species_full_name = 'Lodgepole Pine' WHERE species_cd = 'PL';"
run_sql_r(query, conn_list)
query <- "UPDATE thlb_proxy.vri_species_cd_datadict SET species_full_name = 'Alder' WHERE species_cd = 'D';"
run_sql_r(query, conn_list)
query <- "UPDATE thlb_proxy.vri_species_cd_datadict SET species_full_name = 'Rocky Mtn Juniper' WHERE species_cd = 'JR';"
run_sql_r(query, conn_list)
query <- "UPDATE thlb_proxy.vri_species_cd_datadict SET species_full_name = 'Birch' WHERE species_cd = 'E';"
run_sql_r(query, conn_list)
query <- "UPDATE thlb_proxy.vri_species_cd_datadict SET species_full_name = 'Engelmann spruce' WHERE species_cd = 'SE';"
run_sql_r(query, conn_list)
query <- "UPDATE thlb_proxy.vri_species_cd_datadict SET species_full_name = 'bitter cherry' WHERE species_cd = 'VB';"
run_sql_r(query, conn_list)
query <- "UPDATE thlb_proxy.vri_species_cd_datadict SET species_full_name = 'Other hardwood' WHERE species_cd = 'ZH';"
run_sql_r(query, conn_list)
query <- "UPDATE thlb_proxy.vri_species_cd_datadict SET species_full_name = 'balsam poplar' WHERE species_cd = 'ACB';"
run_sql_r(query, conn_list)
query <- "UPDATE thlb_proxy.vri_species_cd_datadict SET species_full_name = 'red pine' WHERE species_cd = 'PR';"
run_sql_r(query, conn_list)
query <- "UPDATE thlb_proxy.vri_species_cd_datadict SET species_full_name = 'balsam fir' WHERE species_cd = 'BB';"
run_sql_r(query, conn_list)
query <- "UPDATE thlb_proxy.vri_species_cd_datadict SET species_full_name = 'Cedar' WHERE species_cd = 'C';"
run_sql_r(query, conn_list)
query <- "UPDATE thlb_proxy.vri_species_cd_datadict SET species_full_name = 'Sitka x white Spruce' WHERE species_cd = 'SXL';"
run_sql_r(query, conn_list)
query <- "UPDATE thlb_proxy.vri_species_cd_datadict SET species_full_name = 'Douglas-Fir' WHERE species_cd = 'F';"
run_sql_r(query, conn_list)
query <- "UPDATE thlb_proxy.vri_species_cd_datadict SET species_full_name = 'Norway maple' WHERE species_cd = 'MN';"
run_sql_r(query, conn_list)
query <- "UPDATE thlb_proxy.vri_species_cd_datadict SET species_full_name = 'Unknown' WHERE species_cd = 'X';"
run_sql_r(query, conn_list)
query <- "UPDATE thlb_proxy.vri_species_cd_datadict SET species_full_name = 'Scouler''s willow' WHERE species_cd = 'WS';"
run_sql_r(query, conn_list)
query <- "UPDATE thlb_proxy.vri_species_cd_datadict SET species_full_name = 'Unknown conifer' WHERE species_cd = 'XC';"
run_sql_r(query, conn_list)
query <- "UPDATE thlb_proxy.vri_species_cd_datadict SET species_full_name = 'silver birch' WHERE species_cd = 'ES';"
run_sql_r(query, conn_list)
query <- "UPDATE thlb_proxy.vri_species_cd_datadict SET species_full_name = 'Douglas fir' WHERE species_cd = 'FD';"
run_sql_r(query, conn_list)
query <- "UPDATE thlb_proxy.vri_species_cd_datadict SET species_full_name = 'Engelmann x white spruce' WHERE species_cd = 'SXW';"
run_sql_r(query, conn_list)
query <- "UPDATE thlb_proxy.vri_species_cd_datadict SET species_full_name = 'Yew' WHERE species_cd = 'T';"
run_sql_r(query, conn_list)
query <- "UPDATE thlb_proxy.vri_species_cd_datadict SET species_full_name = 'Garry oak' WHERE species_cd = 'QG';"
run_sql_r(query, conn_list)
query <- "UPDATE thlb_proxy.vri_species_cd_datadict SET species_full_name = 'Spruce' WHERE species_cd = 'S';"
run_sql_r(query, conn_list)
query <- "UPDATE thlb_proxy.vri_species_cd_datadict SET species_full_name = 'Willow' WHERE species_cd = 'W';"
run_sql_r(query, conn_list)
query <- "UPDATE thlb_proxy.vri_species_cd_datadict SET species_full_name = 'Unknown hardwood' WHERE species_cd = 'XH';"
run_sql_r(query, conn_list)

### add coniferous/deciduous/unknown types
query <- "alter table thlb_proxy.vri_species_cd_datadict add column type text;"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Deciduous' where species_full_name = 'Alaska Paper Birch';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Deciduous' where species_full_name = 'Alder';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Alpine Larch';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Amabilis Fir';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Deciduous' where species_full_name = 'Arbutus';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'balsam fir';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Deciduous' where species_full_name = 'balsam poplar';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Deciduous' where species_full_name = 'Bigleaf Maple';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Deciduous' where species_full_name = 'Birch';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Deciduous' where species_full_name = 'bitter cherry';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Deciduous' where species_full_name = 'Black Cottonwood';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Black Spruce';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Cedar';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Dahurian Larch';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Douglas-Fir';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Douglas-Fir (Coast)';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Douglas fir';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Douglas Fir (Interior)';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Engelmann spruce';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Engelmann x white spruce';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'European larch/Larix decidua';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Deciduous' where species_full_name = 'Garry oak';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Grand Fir';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Deciduous' where species_full_name = 'Green/Sitka Alder (N)';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Hemlocks';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Jack Pine';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Japanese larch/Larix kaempferi';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Larch';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Limber Pine';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Lodgepole Pine';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Lodgepole Pine (Coast)';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Lodgepole Pine (Interior)';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Mountain Hemlock';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Noble Fir';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Deciduous' where species_full_name = 'Norway maple';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Norway Spruce';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Deciduous' where species_full_name = 'Other hardwood';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Unknown' where species_full_name = 'Other tree, not on list';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Deciduous' where species_full_name = 'Paper Birch';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Pine';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Deciduous' where species_full_name = 'Poplar';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Deciduous' where species_full_name = 'Poplar Hybrid';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Deciduous' where species_full_name = 'Red Alder';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'red pine';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Rocky Mtn Juniper';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Scotch pine/Pinus sylvestris';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Deciduous' where species_full_name = 'Scouler''s willow';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Shasta Red Fir';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Siberian Larch';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Deciduous' where species_full_name = 'silver birch';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Sitka Spruce';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Sitka x white Spruce';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Sitka X/Unknown Hybrid';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Spruce';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Spruce Hybrid';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Sub Alpine Fir';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Tamarack';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Deciduous' where species_full_name = 'Trembling Aspen';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'True Fir';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Unknown' where species_full_name = 'Unknown';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Unknown conifer';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Deciduous' where species_full_name = 'Unknown hardwood';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Western Hemlock';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Western Larch';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Western Red Cedar';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Western White Pine';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'White Spruce';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Whitebark Pine';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Deciduous' where species_full_name = 'Willow';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Yellow Cypress';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Yellow Pine';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Yew';"
run_sql_r(query, conn_list)
query <- "update thlb_proxy.vri_species_cd_datadict set type = 'Coniferous' where species_full_name = 'Norway spruce/Picea abies  ';"
run_sql_r(query, conn_list)