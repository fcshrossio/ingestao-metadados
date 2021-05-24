#!/bin/sh

nohup ./cl-manager-of-ingest.sh -solr_url_repository http://localhost:8983/solr/repositorio/ -sources_file /opt/ingestao-metadados/data/oai_sources.txt -log_file /opt/ingestao-metadados/data/harvest-manager.log.txt -solr_url_search http://localhost:8983/solr/pesquisa/ $* >out-manager-of-ingest.txt 2>&1 &

# -run_index



#!/bin/sh

export CLASSPATH=
for jar in `ls lib/*.jar`
do
  export CLASSPATH=$CLASSPATH:$jar
done
export CLASSPATH=$CLASSPATH

# java -Djsse.enableSNIExtension=false -Dsun.net.inetaddr.ttl=0
java -Xmx6G -cp classes:$CLASSPATH rossio.script.ConvertSourceIds  data/oai-source.ttl http://localhost:8983/solr/repositorio  http://localhost:8983/solr/repositoriov2  >out-convert-source-ids.txt 2>&1 &