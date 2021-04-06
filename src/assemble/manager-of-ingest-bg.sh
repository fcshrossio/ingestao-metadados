#!/bin/sh

nohup ./cl-manager-of-ingest.sh -solr_url_repository http://localhost:8983/solr/repositorio/ -sources_file /opt/rossio-dados/ingestao-metadados/data/oai_sources.txt -log_file /opt/rossio-dados/ingestao-metadados/data/harvest-manager.log.txt -solr_url_search http://localhost:8983/solr/pesquisa/ -run_index $* >out-manager-of-ingest.txt 2>&1 &

