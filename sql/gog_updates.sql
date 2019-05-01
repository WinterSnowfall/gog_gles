-- for entries scanned yesterday, use: date('now', 'localtime', '-1 day', 'start of day')

-- updated ids with changelog
select gp_int_added, gp_id, gp_title, gp_links_product_card, gp_changelog from gog_products 
where gp_int_latest_update like date('now', 'localtime', 'start of day') || '%' and gp_changelog is not null order by 1 desc
-- updated ids with new installer/patch files, but no changelog
select gp_int_added, gf_int_added, gp_id, gf_int_product_name, gp_links_product_card, gf_id, gf_version from gog_products, gog_files 
where gp_id = gf_int_product_id and gf_int_added like date('now', 'localtime', 'start of day') || '%' and gf_int_type = 'installer' order by 1 desc
-- ids that have been removed (optional)
select gp_int_no_longer_listed, gp_int_added, gp_id, gp_title, gp_int_product_url from gog_products 
where gp_int_no_longer_listed like date('now', 'localtime', 'start of day') || '%' order by 1 desc