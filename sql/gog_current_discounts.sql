-- current titles on sale, ordered by discount; additional filtering may be added to narrow down currency and region
SELECT gpr_int_id, gpr_int_title, gpr_int_country_code, gpr_currency, gpr_base_price, gpr_final_price, 
CAST(ROUND((gpr_base_price - gpr_final_price) * 100 / gpr_base_price) AS INTEGER) AS gpr_discount_percent
FROM gog_prices WHERE gpr_final_price < gpr_base_price AND gpr_int_outdated IS NULL ORDER BY 7 DESC;
