package rossio.ingest.solr.manager;

import java.io.File;

import org.oclc.oai.harvester2.verb.ListRecords;

public class Test {

	
	
	public static void main(String[] args) throws Exception {
		ListRecords listRecords = new ListRecords("https://digitarq.adstb.arquivos.pt/oai-pmh/", "oai_dc:::ADSTB:250000", new File("c:\\users\\nfrei\\desktop\\oai.xml"));
	}
}
