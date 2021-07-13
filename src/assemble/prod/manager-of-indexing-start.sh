#!/bin/sh

nohup ./cl-manager-of-indexing.sh -solr_url_repository http://datarossio.dglab.gov.pt:8983/solr/testes-repositorio/ -solr_url_search http://datarossio.dglab.gov.pt:8983/solr/testes-pesquisa/ -sources_file data/oai_sources.ttl -indexing_status_file data/indexing_status.ttl -sparql_vocabs http://vocabs.rossio.fcsh.pt:3030/skosmos/sparql -log_file data/indexing-manager.log.txt $* >out-manager-of-indexing.txt 2>&1 &
