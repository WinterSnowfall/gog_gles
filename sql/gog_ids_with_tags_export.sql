SELECT gp_id AS ID, gp_title AS Title, gp_v2_properties AS Tags FROM gog_products 
WHERE gp_v2_properties IS NOT NULL ORDER BY 1;

