#!/bin/sh

nohup ./cl-manager-of-indexing.sh -solr_url_repository http://192.168.111.170:8983/solr/repositorio/ -solr_url_search http://192.168.111.168:8983/solr/rossio-pesquisa/ -sources_file data/oai_sources.ttl -indexing_status_file data/indexing_status.ttl -sparql_vocabs http://192.168.111.170:3030/skosmos/sparql -log_file data/indexing-manager.log.txt $* >out-manager-of-index.txt 2>&1 & 
