SELECT gfr_name, gfr_int_added FROM gog_forums WHERE UPPER(gfr_name) NOT IN (SELECT UPPER(gp_v2_title) FROM gog_products) ORDER BY 2 DESC;
