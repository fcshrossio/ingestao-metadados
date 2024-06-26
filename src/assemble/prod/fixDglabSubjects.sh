#!/bin/sh

export CLASSPATH=
for jar in `ls lib/*.jar`
do
  export CLASSPATH=$CLASSPATH:$jar
done
export CLASSPATH=$CLASSPATH

# java -Djsse.enableSNIExtension=false -Dsun.net.inetaddr.ttl=0
nohup java -Xmx4G -cp classes:$CLASSPATH rossio.script.ChangeIngestedDatasetsDglab data/oai_sources.ttl http://localhost:8983/solr/repositorio  http://localhost:8983/solr/repositorio  >out-fix-dglab.txt 2>&1 &
