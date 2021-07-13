package rossio.test;

import rossio.ingest.solr.manager.CommandLineManagerOfHarvest;
import rossio.ingest.solr.manager.CommandLineManagerOfIndexing;

public class TestIndexingManager {
	public static void main(String[] args) throws Exception {
		CommandLineManagerOfIndexing.main(new String[] {
		"-solr_url_repository", "http://datarossio.dglab.gov.pt:8983/solr/testes-repositorio",
		"-solr_url_search", "http://datarossio.dglab.gov.pt:8983/solr/testes-pesquisa",
		"-sources_file", "src/data/oai_sources-test-harvest-short.ttl",		
		"-sparql_vocabs", "http://vocabs.rossio.fcsh.pt:3030/skosmos/sparql",		
		"-indexing_status_file", "src/data/indexing_status-test-harvest-short.ttl",		
		"-log_file", "target/indexing-manager.log.txt"
		});
	}

}
