SELECT gp_v2_developer AS "Developer/Publisher" FROM gog_products WHERE gp_v2_developer IS NOT NULL
UNION
SELECT gp_v2_publisher FROM gog_products WHERE gp_v2_publisher IS NOT NULL ORDER BY 1;

