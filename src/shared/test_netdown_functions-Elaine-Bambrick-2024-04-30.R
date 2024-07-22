#netdown functions

# table styling. Argument requires the input of a dataframe, x:
# specifies commas as 1000 separator, font size, caption, etc
pretty_table<-function(x){
  kable(x,
        booktabs = T,
        format.args = list(big.mark = ","),
        caption =  "Netdown Summary Table" ) %>% 
    kable_styling(bootstrap_options = "striped",
                  font_size = 10)
}


# function that looks for items 'not in' a variable/list.
`%notin%` <- Negate(`%in%`) # create a 'not in' list function

# setup the netdown summary table:
# required input is the netdown table
setup_tracking_variable<-function(netdown_tab){
  landclass <- "TSA_Boundary"
  total <- nrow(netdown_tab)
  unit <- nrow(netdown_tab)
  percent <- 100
  excluded <- 0
  running_total <- 0
  netdown<-unit - running_total
  
  netdown_summary <- data.frame(landclass, total, percent, excluded, running_total,netdown)
  
}

# function to pul the 'running_total' value from the netdown summary for a given landclass. RUnning total is the cumulative area removed at a given landclass.
# Required function arguments are a netdown summary table and the landclass

get_running_total<-function(netdown_summary,lc){
  netdown_summary%>%
    filter(landclass == lc)%>%
    select(running_total)%>%
    # pull out the running total 
    pull()
}

# function to pull the netdown value from the netdown summary table.
get_netdown<-function(netdown_summary,lc){
  netdown_summary%>%
    filter(landclass == lc)%>%
    select(netdown)%>%
    pull()
}

# function to update the areas in the netdown table:
# Function arguments: netdown table, variable name.
# update areas 1 is used prior to defining AFLB. Once AFLB is defined, it will not be adjusted following future netdown steps. Move to update_areas2 for updates to thlb_net and gthlb_net variables.
update_areas1 <- function(netdown_tab,var_name) {
  netdown_tab%>%
    # update the aflb, thlb_net and gthlb_net variables. If the variable specified is NA, then leave aflb, thlb_net, and gthlb as is. If the variable is NA then update aflb, thlb_net and gthlb to 0.
    mutate(aflb = if_else(is.na(get(var_name)), aflb, 0),
           thlb_net = if_else(is.na(get(var_name)), thlb_net, 0),
           gthlb_net = if_else(is.na(get(var_name)), gthlb_net, 0))
  
}

# see comments for update_areas1
update_areas2 <- function(netdown_tab,var_name) {
  netdown_tab%>%
    mutate(thlb_net = if_else(is.na(get(var_name)), thlb_net, 0),
           gthlb_net = if_else(is.na(get(var_name)), gthlb_net, 0))
  
}

# similar to update areas 2 - this update function is used to update the thlb after the gross thlb has been defined.
update_areas3 <- function(netdown_tab,var_name) {
  netdown_tab%>%
    mutate(thlb_net = if_else(is.na(get(var_name)), thlb_net, 0))
  
}


# A function to write the pre-thl to postgres. Pre-thlb is used to look at isolated areas before being refined to the final thlb.
# function arguments: netdown table, tsa_lbl, pre_thlb.
create_pre_thlb<-function(netdown_tab,tsa_lbl,pre_thlb){
  # crease an sql statement to create a table (taking ogc_fid and geometry from the skey table and thlb_net, thlb_bi from the netdown table)
  query<-paste0("create table ",
                tsa_lbl,
                "_pre_thlb as (select a.*, b.thlb_net,b.thlb_bi from ",
                tsa_lbl,
                "_skey a join ",
                tsa_lbl,
                "_netdown2023 b using (ogc_fid));")
  # if the table already exists, remove it prior to saving the pre-thlb.
  if(dbExistsTable(db,pre_thlb)) {dbRemoveTable(db,pre_thlb)}
  dbSendQuery(db,query)
  message("pre_thlb table created")
}



# this function is used when the feature being netted out of the land base is categorical or 100% removed.
# a new row is added to the netdown summary table
# function arguments: netdown table, netdown summary, running_total, land class, netdown step
netdown100pct<-function(netdown_tab,net_summary,running_total,lclass,n_step){
  landclass <- lclass
  
  # determine the area of the unit (i.e. number of rows in netdown table)
  unit <- nrow(netdown_tab)
  
  # determine the total area in the netdown step, where n_step is one of the netdown steps (e.g., n01_ownership)
  total <- netdown_tab %>%
    filter(!is.na(get(n_step))) %>% # filter to where netdown step is NOT NA.
    # count number of rows (i.e. area as 1 row = 1 ha)
    count()%>% 
    # pull the count value
    pull()
  
  # determine the percent of TSA area (total of netdown class / total of TSA * 100)
  percent <- total / nrow(netdown_tab) * 100
  
  # determine the area excluded:
  excluded <- netdown_tab %>%
    # filter to where netdown step variable is NOT NA.
    filter(!is.na(get(n_step))) %>%
    # calculate thlb_net area
    summarise(x = sum(thlb_net)) %>%
    pull() # pull value
  
  # calculate new running total value
  running_total <- running_total+excluded
  
  # determine new netdown.
  netdown<-unit - running_total
  
  # create a one-row data frame with each of the netdown summary variables determined for the netdown step.
  netdown_step <- data.frame(landclass, total, percent, excluded, running_total, netdown)
  
  # add the new netdown step to the netdown summary table.
  netdown_summary <- bind_rows(netdown_summary, netdown_step)
  
}

# function for determining the proportional netdown factors.
# ONLY works if landclasses are named "Lineal_Features" or "Retention". 
netdown_prop<-function(netdown_tab,netdown_summary,running_total,lclass,n_step,ret_prop){
  landclass <- lclass
  unit <- nrow(netdown_tab)
  # conditional statement - slightly different calculation depending on whether landclass is lineal features or retention.
  ifelse(landclass == "Linear_Features",
  total <- netdown_tab %>%
    summarise(x = sum(get(n_step))) %>% 
    pull(),
  ifelse(landclass == "Retention",
         total<- netdown_tab %>%
           # multiply included by retention proportion (last step in netdown)
           summarise(x = sum(included * ret_prop))%>%
           pull(),
  total <- netdown_tab %>%
    filter(get(n_step) > 0)%>%
    count()%>%
    pull()))
  
  # calculate percent of total area removed
  percent <- total / nrow(netdown_tab) * 100
  
  # determine the area excluded
  excluded <- netdown_tab %>%
    filter(thlb_net > 0) %>%
    summarise(x = sum(thlb_net * get(n_step))) %>%
    pull()
  
  # calculate running total
  running_total <- running_total + excluded
  
  # calculate netdown
  netdown<-unit - running_total
  
  # create data frame with netdown summary information for given landclass
  netdown_prop<- data.frame(landclass, total, percent, excluded, running_total,netdown)
  
  # add to netdown summary table:
  netdown_summary <- bind_rows(netdown_summary, netdown_prop)
  
}

# this function is used when a landbase summary is needed. I.e., AFLB, grossTHLB, THLB:
landbase_sum<-function(netdown_tab,net_summary,running_total,lclass,netdown){
  landclass <- lclass
  total <- netdown_tab %>%
    summarise(x = sum(thlb_net)) %>%
    pull()
  percent <- total / nrow(netdown_tab) * 100
  excluded<-running_total
  
  
  netdown_landbase <- data.frame(landclass, total, percent, excluded, running_total,netdown)
  netdown_summary <- bind_rows(netdown_summary, netdown_landbase)
  
}

# write tables to postgres.

write_2_postgres<-function(netdown_tab,netdown_summary,net_table,net_summary){

  # netdown table
  # first, remove tables if they exist in the database:  
  if(dbExistsTable(db,net_table)) {dbRemoveTable(db,net_table)}
  # write tables. table names are given in function argument.
  dbWriteTable(db, net_table,netdown_tab,row.names=FALSE)
  message("netdown table loaded to postgres")
  
  # repeat for netdown summary table.
  if(dbExistsTable(db,net_summary)) {dbRemoveTable(db,net_summary)}
  dbWriteTable(db, net_summary,netdown_summary,row.names=FALSE)
  message("netdown summary loaded to postgres")
  
}


# this function creates a bar chart ( with coordinates flipped) of area removed.
# create a chart with areas excluded:
chart_arearemoved <- function(table, var, colours){
  ggplot(table %>% 
               filter(!is.na({{var}}))) +
  aes(x = fct_rev(fct_infreq({{var}})), fill = {{var}}) +
  geom_bar(width = 0.6) +
  scale_fill_manual(values = colours) +
  labs(y = "Area removed (ha)") +
  coord_flip() +
  theme_void() +
  theme(legend.position = "none",
        axis.text.y = element_text(size = 8),
        axis.title.x = element_text(size = 8),
        axis.text.x = element_text(size = 8),
        panel.grid.major.x = element_line(linetype = "dotted", colour = "grey"),
        plot.margin = unit(c(0.25,0.25,0.25,0.25), "cm")) +
  scale_x_discrete(labels = function(x) str_wrap(x, width = 20))+
  scale_y_continuous(labels = comma) 

}

# this function works to add specific layer to basemap (for categorical / 100% netdowns e.g., var = n01...):
map_function <- function(df, var, colours) {
  # create a list of layers to add to the map:
  list(
  # new scale fill for area removed.
  new_scale_fill(),
  # netdown variable:
  geom_sf(data = 
            df %>% 
            filter(!is.na({{var}})) %>% 
            select(ogc_fid, {{var}}) %>% 
            left_join(tsa02_skey, by = "ogc_fid") %>% 
            group_by({{var}}) %>% 
            summarize(geometry = st_union(wkb_geometry)) %>% 
            st_as_sf(), 
          color = NA, aes(fill = {{var}}), alpha = 0.7),
  scale_fill_manual(values = colours),
  labs(fill = ""),
  # lakes
  geom_sf(data = lakes, color = NA, fill = "deepskyblue4", alpha = 0.5), 
  # major rivers:
  geom_sf(data = rivers, col = "deepskyblue3"),
  # major roads:
  geom_sf(data = roads, col = "grey10"),
  # towns:
  geom_sf(data = towns, size = 0.15, color = "grey10", show.legend = FALSE),
  geom_sf_text(data = towns, aes(label = NAME), size = 3, nudge_y = 10, color = "grey10"),
  theme(legend.position = "bottom")
  )
}