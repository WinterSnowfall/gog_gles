-- current titles on sale, ordered by discount; additional filtering may be added to narrow down currency and region
select gpr_id, gpr_product_title, gpr_country_code, gpr_currency, gpr_base_price, gpr_final_price, 
CAST(ROUND((gpr_base_price - gpr_final_price) * 100 / gpr_base_price) as INTEGER) as gpr_discount_percent
from gog_prices where gpr_final_price < gpr_base_price and gpr_int_outdated_on is NULL order by 7 desc