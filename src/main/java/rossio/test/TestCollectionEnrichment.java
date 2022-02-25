package rossio.test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;

import rossio.enrich.metadata.EnrichmentTask;
import rossio.enrich.metadata.RecordEnrichmentNormalizeLinks;
import rossio.ingest.solr.RepositoryWithSolr;
import rossio.util.Global;
import rossio.util.HttpsUtil;

public class TestCollectionEnrichment {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
//		Global.init_componentHttpRequestService();
    	RepositoryWithSolr repository=new RepositoryWithSolr("http://192.168.111.170:8983/solr/repositorio");
//    	RepositoryWithSolr repository=new RepositoryWithSolr("http://datarossio.dglab.gov.pt:8983/solr/repositorio");
		EnrichmentTask enrichTast=new EnrichmentTask();
		enrichTast.testing=true;
//		enrichTast.addEnrichment(new RecordEnrichmentGeo("http://skosmos.dglab.gov.pt:3030/skosmos/sparql"));
//		enrichTast.addEnrichment(new RecordEnrichmentAgents("http://skosmos.dglab.gov.pt:3030/skosmos/sparql"));
//		enrichTast.addEnrichment(new RecordEnrichmentTemporal("http://skosmos.dglab.gov.pt:3030/skosmos/sparql"));
//		enrichTast.addEnrichment(new RecordEnrichmentNormalizeDate());
//		enrichTast.addEnrichment(new RecordEnrichmentNormalizeLanguage());
		enrichTast.addEnrichment(new RecordEnrichmentNormalizeLinks());
		enrichTast.runOnCollection(repository, 
//				"http://biblioteca.teatro-dmaria.pt/OAI/#CARTAZES");
				"https://digitarq.adstr.arquivos.pt/oai-pmh/#ADSTR");
	}

}
