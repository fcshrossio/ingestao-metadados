package rossio.test;

import rossio.ingest.solr.manager.CommandLineManagerOfHarvest;
import rossio.ingest.solr.manager.CommandLineManagerOfIndexing;

public class TestIndexingManager {
	public static void main(String[] args) throws Exception {
		CommandLineManagerOfIndexing.main(new String[] {
		"-solr_url_repository", "http://192.168.111.170:8983/solr/repositorio",
//		"-solr_url_repository", "http://datarossio.dglab.gov.pt:8983/solr/testes-repositorio",
		"-solr_url_search", "http://datarossio.dglab.gov.pt:8983/solr/testes-pesquisa",
//		"-sources_file", "src/data/oai_sources-test-harvest-short.ttl",		
		"-sources_file", "src/data/oai_sources-prod.ttl",		
//		"-sources_file", "src/data/oai_sources_debug.ttl",		
		"-sparql_vocabs", "http://vocabs.rossio.fcsh.unl.pt:3030/skosmos/sparql",		
		"-indexing_status_file", "src/data/indexing_status-prod.ttl",		
//		"-indexing_status_file", "src/data/indexing_status_debug.ttl",		
		"-log_file", "target/indexing-manager.log.txt"
		});
	}

}
