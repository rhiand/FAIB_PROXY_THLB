library(dadmtools)
source('src/utils/functions.R')

conn_list <- dadmtools::get_pg_conn_list()

query <- "DROP TABLE IF EXISTS thlb_proxy.vri_species_cd_datadict;"
run_sql_r(query, conn_list)

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