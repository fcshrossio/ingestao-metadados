package rossio.enrich.metadata;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.system.StreamRDFOps;
import org.apache.solr.client.solrj.SolrServerException;

import rossio.data.models.Rossio;
import rossio.ingest.solr.RepositoryWithSolr;
import rossio.ingest.solr.RepositoryWithSolr.FetchOption;
import rossio.ingest.solr.RepositoryWithSolr.ItemHandler;
import rossio.ingest.solr.manager.OaiSourceIndexStatus;
import rossio.util.RdfUtil.Jena;

public class EnrichmentTask {

	List<RecordEnrichment> enrichments=new ArrayList<RecordEnrichment>();
	
	public boolean testing=false;
	
	
	public EnrichmentTask() {
		super();
	}

	public void addEnrichment(RecordEnrichment enrichment) {
		enrichments.add(enrichment);
	}
	
	public void runOnCollection(RepositoryWithSolr repository, String sourceId) {
    	try {
			repository.getItemsInSource(sourceId, FetchOption.VERSION_AT_SOURCE, new ItemHandler() {
				int recCount=0;
				@Override
				public boolean handle(String uuid, String idAtSource, String lastUpdate, byte[] content, byte[] contentRossio) throws Exception {
					RDFParser reader = RDFParser.create().lang(Lang.RDFTHRIFT).source(new ByteArrayInputStream(content)).build();
					Model model = Jena.createModel();
					reader.parse(model);

					runOnRecord(model.createResource(Rossio.NS_ITEM+uuid));

					return !testing || recCount<20;
				}
			});
		} catch (SolrServerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		
	}

	public void runOnRecord(Resource record) {
		for (RecordEnrichment enrich: enrichments) {
			enrich.enrich(record);
		}
	}
	
	
	public static EnrichmentTask newInstanceForRossio(OaiSourceIndexStatus source, String vocabsSparqlEndpointUrl) {
		EnrichmentTask enrichTask=new EnrichmentTask();
		enrichTask.addEnrichment(new RecordEnrichmentGeo(vocabsSparqlEndpointUrl));
		enrichTask.addEnrichment(new RecordEnrichmentAgents(vocabsSparqlEndpointUrl));
		enrichTask.addEnrichment(new RecordEnrichmentTemporal(vocabsSparqlEndpointUrl));
		enrichTask.addEnrichment(new RecordEnrichmentNormalizeDate());
		enrichTask.addEnrichment(new RecordEnrichmentNormalizeType(vocabsSparqlEndpointUrl));
		enrichTask.addEnrichment(new RecordEnrichmentNormalizeLanguage());
		if(source.enrichLinks())
			enrichTask.addEnrichment(new RecordEnrichmentNormalizeLinks());
		return enrichTask;
	}
}
