package rossio.test;

import rossio.ingest.solr.manager.CommandLineManagerOfHarvest;

public class TestHarvestManager {
	public static void main(String[] args) throws Exception {
		CommandLineManagerOfHarvest.main(new String[] {
		"-solr_url_repository", "http://datarossio.dglab.gov.pt:8983/solr/testes-repositorio",
		"-sources_file", "src/data/oai_sources_debug.ttl",		
//		"-sources_file", "src/data/oai_sources-test-harvest-short.ttl",		
		"-log_file", "target/harvest-manager.log.txt"
		});
	}

}
