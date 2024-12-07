# fyi case 
#     when (LOWER(manlic.forest_file_id) like 'n%' or LOWER(manlic.forest_file_id) like 'w%' or    LOWER(manlic.forest_file_id) like 'k%')
#             and (harvest_restriction_class_name is Null or harvest_restriction_class_name in ('','Medium Restricted','Low Restricted')  )
#             and fmlb_2023 = 'Y' and slp.val::numeric < 60
#         then 1.0
#     when tfl.forest_file_id is not null and 
#     (harvest_restriction_class_name is Null or harvest_restriction_class_name in ('','High Restricted','Medium Restricted','Low Restricted')  or land_designation_type_name = 'Old Growth Management Area (OGMA): Non-Legal')
#         then abt_thlb.thlb_fact::numeric
#     when (harvest_restriction_class_name is Null or harvest_restriction_class_name in ('','High Restricted','Medium Restricted','Low Restricted')  or land_designation_type_name = 'Old Growth Management Area (OGMA): Non-Legal') 
#         then  tsa_thlb.thlb_fact::numeric
#     else 0
# end as thlb_fact