#!/bin/sh

export CLASSPATH=
for jar in `ls lib/*.jar`
do
  export CLASSPATH=$CLASSPATH:$jar
done
export CLASSPATH=$CLASSPATH

# java -Djsse.enableSNIExtension=false -Dsun.net.inetaddr.ttl=0

java -Xmx16G -cp classes:$CLASSPATH rossio.ingest.solr.CommandLineRemoveCollectionFromRepository -solr_url_repository http://192.168.111.170:8983/solr/repositorio/ -sources_file data/oai_sources.ttl -source_id "$@"
# -solr_url_repository --- -source_id ---
