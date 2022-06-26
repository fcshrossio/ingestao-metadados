package rossio.ingest.preprocess;

import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;

import rossio.util.MapOfLists;

public class Mappings {
	
	protected static void addLiteral(Property prop, String elementText, Resource subject, MapOfLists<Property, RDFNode> elementsByProperty) {
		if(!StringUtils.isEmpty(elementText)) 
			elementsByProperty.put(prop, subject.getModel().createLiteral(elementText));
	}

}
