package rossio.ingest.preprocess;

import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.w3c.dom.Element;

import rossio.data.models.DcTerms;
import rossio.util.MapOfLists;

public abstract class CpdocPreprocessor extends CsvMetadataPreprocessor {
	CsvMapping mapping;
	
	protected CpdocPreprocessor(CsvMapping mapping) {
		super();
		this.mapping = mapping;
	}

	protected void processRecord(String uuid, String sourceId, String dataProviderUri, CSVRecord metadata, Resource cho,
			MapOfLists<Property, RDFNode> elementsByProperty) {
		Model m=cho.getModel();
		int idx=-1;
		for(String val: metadata) {
			idx++;
			if(StringUtils.isEmpty(val))
				continue;
			val=val.trim();
			Property propToMap=mapping.getMappingForColumn(idx);
			if(propToMap==null) 
				continue;
			if(propToMap.equals(DcTerms.subject) || propToMap.equals(DcTerms.creator) || propToMap.equals(DcTerms.coverage)) {
				for(String subValue : val.split(";")) {
					subValue=subValue.trim();
					if(!StringUtils.isEmpty(subValue))
						elementsByProperty.put(propToMap, m.createLiteral(subValue));
				}
			} else
				elementsByProperty.put(propToMap, m.createLiteral(val));
		}
	}

	@Override
	public Model preprocess(String uuid, String sourceId, String dataProviderUri, Element metadata) {
		throw new NotImplementedException();
	}
	
}
