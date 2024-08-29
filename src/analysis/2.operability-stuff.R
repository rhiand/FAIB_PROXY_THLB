library(dadmtools)
source('src/utils/functions.R')



conn_list <- dadmtools::get_pg_conn_list()
db <- DBI::dbConnect(conn_list["driver"][[1]],
				host = conn_list["host"][[1]],
				user = conn_list["user"][[1]],
				dbname = conn_list["dbname"][[1]],
				password = conn_list["password"][[1]],
				port = conn_list["port"][[1]])


elevation <- get_dem()
slope <- get_slope(elevation)

# stability<-get_stability(db,stab_query,x)