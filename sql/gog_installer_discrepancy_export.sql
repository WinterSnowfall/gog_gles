SELECT gid_int_added AS Added, gid_int_id AS ID, gid_int_title AS Title, 
gid_int_os AS OS, gid_int_latest_galaxy_build AS "Latest Galaxy build version", 
gid_int_latest_installer_version AS "Latest offline installer version" 
FROM gog_installers_delta WHERE gid_int_fixed IS NULL AND gid_int_false_positive = 0 ORDER BY 1 DESC;

