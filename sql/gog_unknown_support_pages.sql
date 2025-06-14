SELECT gs_name, gs_int_added FROM gog_support WHERE UPPER(gs_name) NOT IN (SELECT UPPER(gp_v2_title) FROM gog_products) ORDER BY 2 DESC;
