package rossio.ingest.preprocess;

import java.util.ArrayList;

import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.NotImplementedException;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Seq;
import org.w3c.dom.Element;

import rossio.data.models.Rossio;
import rossio.ingest.solr.RossioRecord;
import rossio.util.MapOfLists;

public class EfgPreprocessor implements MetadataPreprocessor {

	@Override
	public Model preprocess(String uuid, String sourceId, String dataProviderUri, Element metadata) {
		Model m=RossioRecord.createBaseRecord(uuid, sourceId, dataProviderUri);		
		Resource cho=m.createResource(Rossio.NS_ITEM+uuid);
		MapOfLists<Property, RDFNode> elementsByProperty=new MapOfLists<Property, RDFNode>();
		
		boolean nonAvCreation=metadata.getLocalName().equalsIgnoreCase("nonavcreation");
		if(nonAvCreation)
			MappingsNonAv.preprocessNonAv(uuid, sourceId,dataProviderUri,metadata,cho, elementsByProperty);
		else
			MappingsAv.preprocessAv(uuid, sourceId,dataProviderUri,metadata,cho, elementsByProperty);
		
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
		
//		RdfUtil.printOutRdf(m);
		
		return m;
	}
	
	@Override
	public Model preprocess(String uuid, String sourceId, String dataProviderUri, CSVRecord metadata) {
		throw new NotImplementedException();
	}

}
