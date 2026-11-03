# Spark 3.3 / Scala 2.12 launcher.
# NOTE: libs/nabu-sparkbot-lib-common-assembly-*.jar must be the Scala 2.12 build (adjust the file name below to match).
# NOTE: libs/log4j2.properties must be in log4j 2 syntax (see geoCode/bin/log4j2.properties); Spark 3.3 ignores log4j 1 files.
# AQE is disabled to keep Spark 2.4 plan behaviour (HTTP responses are cached and fan out over repartition(100)).
spark-shell --master yarn \
--packages "com.github.music-of-the-ainur:almaren-framework_2.12:0.9.11-3.3,com.github.music-of-the-ainur:http-almaren_2.12:1.2.8-3.3,com.typesafe:config:1.4.0,com.github.music-of-the-ainur:jdbc-almaren_2.12:0.0.5-3.3,com.oracle.database.jdbc:ojdbc8:19.18.0.0" \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.sql.adaptive.enabled=false \
--conf spark.ui.enabled=false \
--conf spark.driver.geoCodeUrl="" \
--conf spark.driver.timezoneUrl="" \
--conf spark.driver.oracleURL="" \
--conf spark.driver.extraClassPath=ojdbc8.jar:code/geoCode/files/T_EDC_RPT_GOOGLE_TZ_INFO \
--conf spark.driver.schema="" \
--conf spark.driver.batchMode=true \
--conf spark.driver.applicationStatusAPIUrl="" \
--conf spark.driver.oracle_cred="" \
--conf spark.driver.access_key_cred="" \
--conf spark.driver.workload_cred="" \
--driver-memory 5G \
--executor-cores 2 \
--num-executors 5 \
--executor-memory 5G \
--jars libs/ojdbc8.jar,libs/nabu-sparkbot-lib-common-assembly-0.8.2-SNAPSHOT.jar \
--driver-java-options '-Dlog4j.configurationFile=log4j2.properties' \
--files libs/log4j2.properties \
