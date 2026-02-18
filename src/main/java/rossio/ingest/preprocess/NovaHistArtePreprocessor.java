package rossio.ingest.preprocess;

import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.DCTerms;
import org.w3c.dom.Element;

import rossio.util.MapOfLists;

public class NovaHistArtePreprocessor extends CsvMetadataPreprocessor {
	
	public NovaHistArtePreprocessor() {
		mapping.put(0, DCTerms.identifier);
		mapping.put(1, DCTerms.date);
		mapping.put(4, DCTerms.title);
		mapping.put(5, DCTerms.coverage);
		mapping.put(6, DCTerms.relation);
		mapping.put(9, DCTerms.description);
		mapping.put(10, DCTerms.coverage);
		mapping.put(13, DCTerms.relation);
		linesToSkip=1;
	}

	
	protected void processRecord(String uuid, String sourceId, String dataProviderUri, CSVRecord metadata, Resource cho,
			MapOfLists<Property, RDFNode> elementsByProperty) {
		Model m=cho.getModel();
		for(int idx=0; idx < metadata.size(); idx++) {
			Property propToMap=mapping.get(idx);
			if(propToMap==null) 
				continue;			
	     String val=metadata.get(idx);
       if(StringUtils.isEmpty(val))
	        continue;
	     val=val.trim();
			if(idx==1) { 
			  String val2=metadata.get(2).trim();
			  if(!val.equals(val2))
			    val=val+"/"+val2;
			}else if(idx==5) { 
			  String val2=metadata.get(7).trim();
			  if(!StringUtils.isEmpty(val2))
			    val=val+", "+val2;
			}else if(idx==10) { 
			  String val2=metadata.get(11).trim();
			  if(!StringUtils.isEmpty(val2))
			    val=val+", "+val2;
			}
			elementsByProperty.put(propToMap, m.createLiteral(val));
		}
	}

	@Override
	public Model preprocess(String uuid, String sourceId, String dataProviderUri, Element metadata) {
		throw new NotImplementedException();
	}
	
}
