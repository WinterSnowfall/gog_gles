SELECT gp_id AS ID, gp_title AS Title, gp_game_type AS Type FROM gog_products 
WHERE gp_int_delisted IS NULL ORDER BY 1

