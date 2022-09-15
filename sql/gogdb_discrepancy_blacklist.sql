SELECT gid_int_id, gid_int_os, gid_int_latest_galaxy_build, gid_int_latest_installer_version, gid_int_title, gid_int_false_positive_reason 
FROM gog_installers_delta WHERE gid_int_false_positive = 1 ORDER BY 1 ASC;

