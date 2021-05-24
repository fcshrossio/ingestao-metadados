#!/bin/sh

export CLASSPATH=
for jar in `ls lib/*.jar`
do
  export CLASSPATH=$CLASSPATH:$jar
done
export CLASSPATH=$CLASSPATH

# java -Djsse.enableSNIExtension=false -Dsun.net.inetaddr.ttl=0
java -Xmx6G -cp classes:$CLASSPATH rossio.ingest.solr.manager.CommandLineManagerOfIndexing "$@"

