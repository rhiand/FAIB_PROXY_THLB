library(RPostgres)
library(glue)
library(devtools)
# install_github("bcgov/FAIB_DADMTOOLS")
library(dadmtools)
source('src/utils/functions.R')
## relies on install_github("bcgov/FAIB_DATA_MANAGEMENT") being installed at some point
conn_list <- dadmtools::get_pg_conn_list()


## requires the following function: 
## seral.faib_seral_bdg_ndt3_140
query <- "DROP FUNCTION IF EXISTS thlb_proxy.faib_seral_bdg_ndt3_140(text, integer, text, text, text, text);"
run_sql_r(query, conn_list)

query <- "CREATE OR REPLACE FUNCTION thlb_proxy.faib_seral_bdg_ndt3_140(
	fmlb text,
	age integer,
	ndt text,
	bec_zone text,
	bclcs_1 text,
	btm text)
    RETURNS text
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$

---------------------------------------------------------------------------------------------------------------------------------------
declare 
	seral text;
begin
	case 
		when fmlb::boolean then    
			case 
				when not ((coalesce(bclcs_1,'') in ('U', ''))) then 
					case 
						when (upper(ndt) in ('NDT1') and upper(bec_zone) in ('CWH','ICH','ESSF','MH') or 
							  upper(ndt) in ('NDT2') and upper(bec_zone) in ('CWH','CDF', 'ICH','SBS','ESSF','SWB') or 
							  upper(ndt) in ('NDT4') and upper(bec_zone) in ('ICH','IDF', 'PP')) then  
							case
								when age > 250 then seral = 'Old';
								when age <  40 then seral = 'Early';
								else
									case 
									----------------------------------------------------------------------------			
										when upper(ndt) in ('NDT1') then 
											case 
												when upper(bec_zone) in ('CWH') then 
													case 
													 	when age > 80 then 					seral = 'Mature';
													 	when age >= 40 and age <= 80 then 	seral = 'Mid';
													 	else seral = null;
													end case;
												when upper(bec_zone) in ('ICH') then 
													case 
													 	when age > 100 then 				seral = 'Mature';
													 	when age >= 40 and age <= 100 then 	seral = 'Mid';
													 	else seral = null;
													end case;
												when upper(bec_zone) in ('ESSF', 'MH') then 
													case 
													 	when age > 120 then 				seral = 'Mature';
													 	when age >= 40 and age <= 120 then 	seral = 'Mid';
													 	else seral = null;
													end case;		
												else seral = null;
						  					end case;
						  			----------------------------------------------------------------------------			
						  				when upper(ndt) in ('NDT2') then 
											case 
												when upper(bec_zone) in ('CWH','CDF') then 
													case 
													 	when age > 80 then 					seral = 'Mature';
													 	when age >= 40 and age <= 80 then 	seral = 'Mid';
													 	else seral = null;
													end case;
												when upper(bec_zone) in ('ICH','SBS') then 
													case 
													 	when age > 100 then 				seral = 'Mature';
													 	when age >= 40 and age <= 100 then 	seral = 'Mid';
													 	else seral = null;
													end case;
												when upper(bec_zone) in ('ESSF', 'SWB') then 
													case 
													 	when age > 120 then 				seral = 'Mature';
													 	when age >= 40 and age <= 120 then 	seral = 'Mid';
													 	else seral = null;
													end case;	
												else seral = null;	
						  					end case;
						  			----------------------------------------------------------------------------			
						  				when upper(ndt) in ('NDT4') then 
											case 
												when upper(bec_zone) in ('ICH','IDF','PP') then 
													case 
													 	when age > 100 then 				seral = 'Mature';
													 	when age >= 40 and age <= 100 then 	seral = 'Mid';
													 	else seral = null;
													end case;
												else seral = null;
						  					end case;		
						  				else seral = null;
						  			end case;
							end case;		
					  	when (upper(ndt) in ('NDT3') and upper(bec_zone) in ('BWBS','SBPS','SBS','MS','ICH','ESSF','CWH')) then  
					  		case 
					  			when age > 140 then seral = 'Old';
								when age <  40 then seral = 'Early';
								else 
							  		case 
										when upper(bec_zone) in ('BWBS','SBPS','SBS','MS','ICH') then 
											case 
											 	when age > 100 then					seral = 'Mature';
											 	when age >= 40 and age <= 100 then 	seral = 'Mid';
											 	else seral = null;
											end case;
										when upper(bec_zone) in ('ESSF') then 
											case 
											 	when age > 120 then 				seral = 'Mature';
											 	when age >= 40 and age <= 120 then 	seral = 'Mid';
											 	else seral = null;
											end case;
										when upper(bec_zone) in ('CWH') then 
											case 
											 	when age > 80 then 					seral = 'Mature';
											 	when age >= 40 and age <= 80 then 	seral = 'Mid';
											 	else seral = null;
											end case;
										else seral = null;		
		  							end case;
		  					end case;
		  				else seral = null;
					end case; 
				else 
					case 
						when age <= 40 then 										seral = 'Early';
						when btm in ('Old Forest') then 							seral = 'Old';
						when btm in ('Selectively Logged') then 					seral = 'Mature';
						when btm in ('Young Forest') then 							seral = 'Mid';
						when btm in ('Recently Logged') then 						seral = 'Early';
						else seral = null;
					end case;
			end case;
		else seral = null;
	end case;
	return seral;
end; 
$BODY$;"
run_sql_r(query, conn_list)

## Rationale: channel width needed to calculate riparian buffers
## Data source from Simon Norris/Craig Mount
## see data\raw\raw_README.txt for further details
query <- "DROP TABLE IF EXISTS thlb_proxy.f_own_falb_lut;"
run_sql_r(query, conn_list)
query <- "CREATE TABLE thlb_proxy.f_own_falb_lut
(
    own smallint,
    ownership_desc text,
    falb boolean
)
TABLESPACE pg_default;"
run_sql_r(query, conn_list)
query <- "COPY thlb_proxy.f_own_falb_lut from 'C:\\projects\\THLB_Proxy\\data\\input\\MikeFowler-f_own_falb_lut.csv' CSV HEADER"
run_sql_r(query, conn_list)