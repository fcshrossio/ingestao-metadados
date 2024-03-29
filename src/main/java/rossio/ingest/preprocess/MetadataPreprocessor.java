package rossio.ingest.preprocess;

import org.apache.commons.csv.CSVRecord;
import org.apache.jena.rdf.model.Model;
import org.w3c.dom.Element;

public interface MetadataPreprocessor {

	public Model preprocess(String uuid, String sourceId, String dataProviderUri, Element metadata);
	
	public Model preprocess(String uuid, String sourceId, String dataProviderUri, CSVRecord metadata);

}
