package rossio.test;

import java.io.File;

import rossio.ingest.solr.manager.CommandLineManagerOfHarvest;

public class TestHarvestManager {
	public static void main(String[] args) throws Exception {
//		String confFile="src/data/oai_sources_debug_cesem.ttl";
		String confFile="src/data/oai_sources_cinemateca_file.ttl";
//		String confFile="src/data/oai_sources_debug_coimbra.ttl";
//		String confFile="src/data/oai_sources_debug_cinemateca.ttl";
		
		File lock=new File(confFile+".lock");
		if(lock.exists())
			lock.delete();
		
		CommandLineManagerOfHarvest.main(new String[] {
		"-solr_url_repository", "http://192.168.111.170:8983/solr/testes-repositorio",
		"-sources_file", confFile,		
//		"-sources_file", "src/data/oai_sources_debug_madeira.ttl",		
//		"-sources_file", "src/data/oai_sources-test-harvest-short.ttl",		
		"-log_file", "target/harvest-manager.log.txt"
		});
	}

}
