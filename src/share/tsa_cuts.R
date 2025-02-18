library(dadmtools)
conn_list <- dadmtools::get_pg_conn_list()
setwd('C:/projects/THLB_Proxy/data/output/')

tsas <- c('27', '43', '17', '02')
tsa_names <- c('Revelstoke TSA', 'Nass TSA', 'Robson Valley TSA', 'Boundary TSA')


for (i in 1:length(tsas)) {
	tsa <- tsas[i]
	tsa_name <- tsa_names[i]
	query <- glue("DROP TABLE IF EXISTS public.thlb_proxy_netdown_tsa{tsa}")
	run_sql_r(query, conn_list)

	query <- glue("CREATE TABLE public.thlb_proxy_netdown_tsa{tsa} AS SELECT * FROM thlb_proxy.prov_netdown WHERE man_unit = '{tsa_name}';")
	print(query)
	run_sql_r(query, conn_list)

	# dump_cmd <- glue("bash -c 'pg_dump -d prov_data -U postgres -h 142.36.123.95 -t public.thlb_proxy_netdown_tsa{tsa} > thlb_proxy_netdown_tsa{tsa}.sql'")
	# system(dump_cmd)

	# query <- glue("DROP TABLE IF EXISTS public.thlb_proxy_netdown_tsa{tsa}")
	# run_sql_r(query, conn_list)
}
