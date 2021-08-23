package rossio.test;

import rossio.export.CommandLineDatasetExporter;

public class TestDatasetExporter {
	public static void main(String[] args) throws Exception {
		CommandLineDatasetExporter.main(new String[] {
		"-solr_url_repository", "http://datarossio.dglab.gov.pt:8983/solr/testes-repositorio",
		"-sources_file", "src/data/oai_sources_debug.ttl",		
//		"-sources_file", "src/data/oai_sources-test-harvest-short.ttl",		
		"-export_folder", "C:\\Users\\nfrei\\Desktop\\ROSSIO_Exports\\dataset",
		"-source_id", "https://arouca.fcsh.unl.pt/fontes/oai#",
//		"-sample_size", "100",
		});
		
		
	}

}
