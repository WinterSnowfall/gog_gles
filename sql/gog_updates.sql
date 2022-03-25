-- for entries scanned yesterday, use: DATE('now', 'localtime', '-1 day', 'start of day')

-- updated ids with changelog
SELECT gp_int_added, gp_id, gp_title, gp_links_product_card, gp_changelog FROM gog_products 
WHERE gp_int_latest_update LIKE DATE('now', 'localtime', 'start of day') || '%' AND gp_changelog IS NOT null ORDER BY 1 DESC;
-- updated ids with new installer/patch files, but no changelog
SELECT gp_int_added, gf_int_added, gp_id, gf_int_product_name, gp_links_product_card, gf_id, gf_version FROM gog_products, gog_files 
WHERE gp_id = gf_int_product_id AND gf_int_added LIKE DATE('now', 'localtime', 'start of day') || '%' AND gf_int_type = 'installer' ORDER BY 1 DESC;
-- ids that have been removed (optional)
SELECT gp_int_no_longer_listed, gp_int_added, gp_id, gp_title, gp_int_product_url FROM gog_products 
WHERE gp_int_no_longer_listed LIKE DATE('now', 'localtime', 'start of day') || '%' ORDER BY 1 DESC;

