package rossio.ingest.preprocess;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.csv.CSVRecord;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.w3c.dom.Element;

public abstract class CsvMetadataPreprocessor implements MetadataPreprocessor {
	Map<Integer, Property> mapping=new HashMap<Integer, Property>();
	
	public abstract Model preprocess(String uuid, String sourceId, String dataProviderUri, CSVRecord metadata);

}
