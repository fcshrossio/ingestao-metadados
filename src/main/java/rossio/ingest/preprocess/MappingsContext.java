package rossio.ingest.preprocess;

import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.w3c.dom.Element;

import rossio.data.models.DcTerms;
import rossio.util.MapOfLists;
import rossio.util.XmlUtil;

public class MappingsContext {

	protected static void processItem(Resource subject, Element xmlElement, MapOfLists<Property,RDFNode> elementsByProperty) {
		String v=XmlUtil.getElementTextByTagNameIgnoreCase(xmlElement,"Type");
		if(!StringUtils.isEmpty(v)) 
			elementsByProperty.put(DcTerms.type, subject.getModel().createLiteral(v));										
		v=XmlUtil.getElementTextByTagNameIgnoreCase(xmlElement,"URI");
		if(!StringUtils.isEmpty(v)) 
			elementsByProperty.put(DcTerms.identifier, subject.getModel().createLiteral(v));										
		v=XmlUtil.getElementTextByTagNameIgnoreCase(xmlElement,"isShownBy");
		if(!StringUtils.isEmpty(v)) 
			elementsByProperty.put(DcTerms.identifier, subject.getModel().createLiteral(v));										
		
//		for (Element subXmlElement: XmlUtil.elements(xmlElement)) {
//			if(subXmlElement.getLocalName().equalsIgnoreCase("URI") || subXmlElement.getLocalName().equalsIgnoreCase("isShownBy")
//					|| subXmlElement.getLocalName().equalsIgnoreCase("isShownAt")) {
//				v=XmlUtil.getElementText(subXmlElement);
//				if(!StringUtils.isEmpty(v)) 
//					elementsByProperty.put(DcTerms.identifier, subject.getModel().createLiteral(v));
//			}
//		}
	}

	protected static void processRelAgent(Resource subject, Element xmlElement, MapOfLists<Property,RDFNode> elementsByProperty) {
		String v=XmlUtil.getElementTextByTagNameIgnoreCase(xmlElement,"efg:name");
		if(!StringUtils.isEmpty(v)) {
			String role=XmlUtil.getElementTextByTagNameIgnoreCase(xmlElement,"efg:type");
			if(!StringUtils.isEmpty(role) && (role.equals("Autor") || role.equals("Realizador") || role.equals("Realização")))
				elementsByProperty.put(DcTerms.creator, subject.getModel().createLiteral(v));
			else
				elementsByProperty.put(DcTerms.contributor, subject.getModel().createLiteral(v));
		}
	}

	protected static void processCredits(Resource subject, Element xmlElement, MapOfLists<Property,RDFNode> elementsByProperty) {
		for (Element subXmlElement: XmlUtil.elements(xmlElement)) {
			if(subXmlElement.getLocalName().equalsIgnoreCase("Person") || subXmlElement.getLocalName().equalsIgnoreCase("CorporateBody")) {
				String v=XmlUtil.getElementTextByTagNameIgnoreCase(xmlElement,"Name");
				if(!StringUtils.isEmpty(v)) {
					String role=XmlUtil.getElementTextByTagNameIgnoreCase(xmlElement,"TypeOfActivity");
					if(!StringUtils.isEmpty(role) && role.equals("Distribuidor"))
						elementsByProperty.put(DcTerms.publisher, subject.getModel().createLiteral(v));
					else
						elementsByProperty.put(DcTerms.contributor, subject.getModel().createLiteral(v));
				}
			}
		}
	}
	


	protected static void processAgent(Resource subject, Element xmlElement, MapOfLists<Property,RDFNode> elementsByProperty) {
		for (Element subXmlElement: XmlUtil.elements(xmlElement)) {
			if(subXmlElement.getLocalName().equalsIgnoreCase("Person") || subXmlElement.getLocalName().equalsIgnoreCase("CorporateBody")) {
				String v=XmlUtil.getElementTextByTagNameIgnoreCase(xmlElement,"efg:Name");
				if(!StringUtils.isEmpty(v)) {
					String role=XmlUtil.getElementTextByTagNameIgnoreCase(xmlElement,"efg:TypeOfActivity");
					if(!StringUtils.isEmpty(role) && (role.equals("Realizador") || role.equals("Realização")))
						elementsByProperty.put(DcTerms.creator, subject.getModel().createLiteral(v));
					else
						elementsByProperty.put(DcTerms.contributor, subject.getModel().createLiteral(v));
				}
			}
		}
	}
}
