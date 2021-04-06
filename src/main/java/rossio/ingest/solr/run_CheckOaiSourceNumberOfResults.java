package rossio.ingest.solr;

import java.io.File;
import java.io.IOException;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.solr.client.solrj.SolrServerException;

import rossio.ingest.solr.manager.Logger;
import rossio.ingest.solr.manager.OaiSource;
import rossio.ingest.solr.manager.OaiSources;
import rossio.oaipmh.HarvestReport;
import rossio.oaipmh.OaiPmhRecord;
import rossio.oaipmh.OaipmhHarvest;

public class run_CheckOaiSourceNumberOfResults {

	public static void main(String[] args) throws Exception {
		
		StringBuffer results=new StringBuffer();
		
		OaiSources oaiSources = new OaiSources(new File("src/data/oai_sources.txt"));

		for (OaiSource src : oaiSources.getAllSources()) {
			System.out.print(src.getSourceId() + " ");

			OaipmhHarvest harvest;
			harvest = new OaipmhHarvest(src.baseUrl, src.metadataPrefix, src.set);
			System.out.println(harvest.getCompleteListSize());
			
			results.append(src.getSourceId()).append(",").append(harvest.getCompleteListSize()).append("\n");
		}
		System.out.println("Finished:\n"+results);

	}
}
