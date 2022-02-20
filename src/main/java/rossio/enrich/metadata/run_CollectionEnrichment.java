package rossio.enrich.metadata;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;

import rossio.ingest.solr.RepositoryWithSolr;

public class run_CollectionEnrichment {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
//    	RepositoryWithSolr repository=new RepositoryWithSolr("http://192.168.111.115:8983/solr/repositorio");
    	RepositoryWithSolr repository=new RepositoryWithSolr("http://datarossio.dglab.gov.pt:8983/solr/repositorio");
		EnrichmentTask enrichTast=new EnrichmentTask();
		enrichTast.testing=true;
//		enrichTast.addEnrichment(new RecordEnrichmentGeo("http://skosmos.dglab.gov.pt:3030/skosmos/sparql"));
//		enrichTast.addEnrichment(new RecordEnrichmentAgents("http://skosmos.dglab.gov.pt:3030/skosmos/sparql"));
//		enrichTast.addEnrichment(new RecordEnrichmentTemporal("http://skosmos.dglab.gov.pt:3030/skosmos/sparql"));
//		enrichTast.addEnrichment(new RecordEnrichmentNormalizeDate());
		enrichTast.addEnrichment(new RecordEnrichmentNormalizeLanguage());
		enrichTast.runOnCollection(repository, 
				"http://biblioteca.teatro-dmaria.pt/OAI/#CARTAZES");
	}

}
