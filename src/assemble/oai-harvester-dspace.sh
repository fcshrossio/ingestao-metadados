#!/bin/sh

export CLASSPATH=
for jar in `ls lib/*.jar`
do
  export CLASSPATH=$CLASSPATH:$jar
done
export CLASSPATH=$CLASSPATH

echo java -Djsse.enableSNIExtension=false -Dsun.net.inetaddr.ttl=0 -Xmx16G -cp classes:$CLASSPATH rossio.ingest.dspace.CommandLineInterface "$@"
java -Xmx16G -cp classes:$CLASSPATH rossio.ingest.dspace.CommandLineOaiHarvester "$@"