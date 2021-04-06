package rossio.enrich.metadata;

import org.apache.jena.rdf.model.Resource;

public interface RecordEnrichment {
	public void enrich(Resource record) ;
}
