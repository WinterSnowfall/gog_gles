-- for entries scanned yesterday, use: DATE('now', 'localtime', '-1 day', 'start of day')

-- updated ids with changelog
SELECT gp_int_added, gp_id, gp_v2_title, gp_changelog FROM gog_products 
WHERE gp_int_updated LIKE DATE('now', 'localtime', 'start of day') || '%' AND gp_changelog IS NOT null ORDER BY 1 DESC;

-- updated ids with new installer/patch files, but no changelog
SELECT gp_int_added, gf_int_added, gp_id, gf_name, gf_id, gf_version FROM gog_products, gog_files 
WHERE gp_id = gf_int_id AND gf_int_added LIKE DATE('now', 'localtime', 'start of day') || '%' AND gf_type = 'installer' ORDER BY 1 DESC;

-- ids that have been removed (optional)
SELECT gp_int_delisted, gp_int_added, gp_id, gp_v2_title FROM gog_products 
WHERE gp_int_delisted LIKE DATE('now', 'localtime', 'start of day') || '%' ORDER BY 1 DESC;
