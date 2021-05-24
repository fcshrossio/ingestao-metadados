#!/bin/sh

export CLASSPATH=
for jar in `ls lib/*.jar`
do
  export CLASSPATH=$CLASSPATH:$jar
done
export CLASSPATH=$CLASSPATH

# java -Djsse.enableSNIExtension=false -Dsun.net.inetaddr.ttl=0
# -solr_url_search --- -source_id ---
java -Xmx16G -cp classes:$CLASSPATH rossio.ingest.solr.CommandLineRemoveCollectionFromSearch "$@"

