#!/bin/sh

nohup ./cl-manager-of-ingest.sh -solr_url_repository http://192.168.111.170:8983/solr/repositorio/ -sources_file data/oai_sources.ttl -log_file data/harvest-manager.log.txt $* >out-manager-of-ingest.txt 2>&1 &


