package rossio.test;

import java.io.File;

import rossio.ingest.solr.manager.CommandLineManagerOfHarvest;

public class TestHarvestManager {
	public static void main(String[] args) throws Exception {
		File lock=new File("src/data/oai_sources_debug.ttl.lock");
		if(lock.exists())
			lock.delete();
		
		CommandLineManagerOfHarvest.main(new String[] {
		"-solr_url_repository", "http://datarossio.dglab.gov.pt:8983/solr/testes-repositorio",
		"-sources_file", "src/data/oai_sources_debug.ttl",		
//		"-sources_file", "src/data/oai_sources-test-harvest-short.ttl",		
		"-log_file", "target/harvest-manager.log.txt"
		});
	}

}
