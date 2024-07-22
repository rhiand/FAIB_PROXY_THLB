library(dadmtools)
import_bcgw_to_pg <- function(
                               src_schema    = "WHSE_BASEMAPPING",
                               src_layer     = "GBA_RAILWAY_TRACKS_SP",
							   fdw_schema    = "load",
							   dst_schema    = "whse_sp",
							   dst_layer     = "GBA_RAILWAY_TRACKS_SP",
                               layer_id      = "railway_track_id",
							   geometry_name = "shape",
							   geometry_type = "MultiLineString",
							   grouping_name = "railway",
							   pg_conn_list

) {
	query <- glue('DROP FOREIGN TABLE IF EXISTS {fdw_schema}.{src_layer};')
	run_sql_r(query, pg_conn_list)
	query <- glue('IMPORT FOREIGN SCHEMA "{src_schema}" LIMIT TO ({src_layer}) FROM SERVER oradb INTO {fdw_schema};')
	run_sql_r(query, pg_conn_list)
	query <- glue('DROP TABLE IF EXISTS {dst_schema}.{dst_layer};')
	run_sql_r(query, pg_conn_list)
	if (is.null(grouping_name)){
	query <- glue("CREATE TABLE {dst_schema}.{dst_layer} as
	SELECT
		{layer_id},
		ST_Force2d({geometry_name})::geometry({geometry_type}, 3005) as geom
	FROM
		{fdw_schema}.{src_layer};")
	} else {
	query <- glue("CREATE TABLE {dst_schema}.{dst_layer} as
	SELECT
		{layer_id},
		'{grouping_name}'::text as grouping_name,
		ST_Force2d({geometry_name})::geometry({geometry_type}, 3005) as geom
	FROM
		{fdw_schema}.{src_layer};")
	}
	run_sql_r(query, pg_conn_list)
	query <- glue('CREATE INDEX {dst_layer}_geom_idx on {dst_schema}.{dst_layer} USING gist(geom);')
	run_sql_r(query, pg_conn_list)
}

check_id_in_df <- function(df, id, grouping_id) {
  # Check if there are any rows where waterbody_poly_id equals current_waterbody_poly_id
  if (is.null(grouping_id)){
    any(df$waterbody_poly_id == id)
  } else {
	any(df$waterbody_poly_id == id & df$grouping_id == grouping_id)
  }
}


remove_from_list <- function(ids_to_check, id) {
  keep <- sapply(ids_to_check, function(x) x != id)
  return(ids_to_check[keep])
}

group_overlapping_ids <- function(df, field1, field2) {
  ## create an empty df and name the columns
  new_df <- data.frame(matrix(ncol = 2, nrow = 0))
  names(new_df) <- c('grouping_id', field1)

  # Loop through each row of the original data frame
  for (wetland_i in 1:nrow(df)) {
    # Get the current row's waterbody_poly_id and overlapping_waterbody_poly_id
    # ensure to handle if there are multiple waterbody_poly_id's that match the waterbody_poly_id of i
    matching_rows <- which(df[, field1] == df[, field1][wetland_i])
    current_waterbody_poly_id <- df[, field1][wetland_i]
    current_overlapping_waterbody_poly_id <- df[, field2][matching_rows]
  	## if new_df already has an instance on the current waterbody_poly_id, skip
    if (check_id_in_df(new_df, current_waterbody_poly_id, grouping_id=NULL)) {
    	next
    }
    # Append the current grouping_id and waterbody_poly_id to the new data frame
    # print(glue('Appending A {wetland_i}, {current_waterbody_poly_id}'))
    new_df <- rbind(new_df, data.frame(grouping_id = wetland_i, setNames(list(current_waterbody_poly_id), field1)))
    # Append the current grouping_id and current_waterbody_poly_id(s) to the new data frame
    # print(glue('Appending A {rep(wetland_i, length(current_overlapping_waterbody_poly_id))}, {current_overlapping_waterbody_poly_id}'))
	new_rows <- data.frame(
  							grouping_id = rep(wetland_i, length(current_overlapping_waterbody_poly_id)),
  							setNames(list(current_overlapping_waterbody_poly_id), field1)
	)		
    new_df <- rbind(new_df, new_rows)
  	
    # Assign overlapping ids to a list
    if (length(current_overlapping_waterbody_poly_id) > 1){
      ids_to_check <- as.list(current_overlapping_waterbody_poly_id)
    } else {
  	  ids_to_check <- list(current_overlapping_waterbody_poly_id)
    }
	## Iterate over the overlapping ids in a recursive like loop to capture the ids that overlap
	## the iteration ensure to capture the entire wetland complex
    while (length(ids_to_check) > 0) {
      for (id_to_check in ids_to_check) {
  		
  		# print(glue('Assessing id: {id_to_check}'))
        # Check for any records in waterbody_poly_id that match the current overlapping_waterbody_poly_id
        matching_rows <- which(df[, field1] == id_to_check)
        # If there are matching rows, append their overlapping_waterbody_poly_id to the new data frame with the initial grouping_id
        if (length(matching_rows) > 0) {
  		  current_overlapping_waterbody_poly_id <- df[, field2][matching_rows]
  		  if (length(current_overlapping_waterbody_poly_id) > 1){
  		  	new_ids_to_check <- as.list(current_overlapping_waterbody_poly_id)
  		  } else {
  		  	new_ids_to_check <- list(current_overlapping_waterbody_poly_id)
  		  }
  		  # Append the current grouping_id and overlap_id to the new data frame
  		  for (overlap_id in new_ids_to_check) {
  		    ## only rbind if it doesn't already exist
  		    if (!(check_id_in_df(new_df, overlap_id, wetland_i))) {
  		  	# print(glue('Appending L {wetland_i}, {overlap_id}'))
		  	new_df <- rbind(new_df, data.frame(grouping_id = wetland_i, setNames(list(overlap_id), field1)))
  		  	# print(glue('Adding {overlap_id} to ids_to_check'))
  		  	ids_to_check <- append(ids_to_check, list(overlap_id))
  		    }	
  		  }
  		  # remove id from ids to check
  		  # print(glue('removing a {id_to_check} from ids_to_check'))
  		  ids_to_check <- remove_from_list(ids_to_check, id_to_check)
        } else {
  		  # print(glue('removing b {id_to_check} from ids_to_check'))
  		  ids_to_check <- remove_from_list(ids_to_check, id_to_check)
  	    }
      }
    }
  }
  return(new_df)
}

## Test out group_overlapping_ids function
# test_input <- data.frame(
#   waterbody_poly_id             = c(10, 10, 11, 21, 20, 20, 24, 55, 22, 22, 22, 20),
#   overlapping_waterbody_poly_id = c(20, 21, 22, 23, 24, 25, 55, 66, 88, 89, 11, 10)
# )

# expected_results <- data.frame(
# 	grouping_id       = c(1 ,1 ,1 ,1 ,1 ,1 ,1 ,1 ,3 ,3 ,3 ,3 ),
# 	waterbody_poly_id = c(10,20,21,24,25,23,55,66,11,22,88,89)
# )

# results <- group_overlapping_ids(test_input, 'waterbody_poly_id', 'overlapping_waterbody_poly_id')
# all.equal(results, expected_results)
