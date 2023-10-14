package rossio.ingest.preprocess;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.csv.CSVRecord;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Seq;
import org.w3c.dom.Element;

import rossio.data.models.Rossio;
import rossio.ingest.solr.RossioRecord;
import rossio.util.MapOfLists;
import rossio.util.RdfUtil;

public abstract class CsvMetadataPreprocessor implements MetadataPreprocessor {
	Map<Integer, Property> mapping=new HashMap<Integer, Property>();
	int linesToSkip=0;
	
	private int currentLine=0;
	
//	public abstract Model preprocess(String uuid, String sourceId, String dataProviderUri, CSVRecord metadata);
	@Override
	public Model preprocess(String uuid, String sourceId, String dataProviderUri, CSVRecord metadata) {
		currentLine++;
		if(currentLine<=linesToSkip)
			return null;
		
		Model m=RossioRecord.createBaseRecord(uuid, sourceId, dataProviderUri);		
		Resource cho=m.createResource(Rossio.NS_ITEM+uuid);
		MapOfLists<Property, RDFNode> elementsByProperty=new MapOfLists<Property, RDFNode>();
		
		processRecord(uuid, sourceId,dataProviderUri,metadata, cho, elementsByProperty);
		
		for (Property property: elementsByProperty.keySet()) {
			ArrayList<RDFNode> values = elementsByProperty.get(property);
			if(values.size()>1) {
				Seq seq=m.createSeq();
				for (RDFNode value: values) {
					seq.add(value);
				}
				m.add(m.createStatement(cho, property, seq));
			}else {
				RDFNode value=values.get(0);
				m.add(m.createStatement(cho, property, value));
			}
		}
		RdfUtil.printOutRdf(m);
		return m;
	}
	
	protected abstract void processRecord(String uuid, String sourceId, String dataProviderUri, CSVRecord metadata, Resource cho,
			MapOfLists<Property, RDFNode> elementsByProperty);
}
