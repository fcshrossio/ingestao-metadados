package rossio.ingest.preprocess;

import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.w3c.dom.Element;

import rossio.data.models.DcTerms;
import rossio.util.MapOfLists;
import rossio.util.XmlUtil;

public class MappingsNonAvOai {

	protected static void preprocessNonAv(String uuid, String sourceId, String dataProviderUri, Element metadata, Resource cho, MapOfLists<Property,RDFNode> elementsByProperty) {
		Model m=cho.getModel();
		for (Element xmlElement: XmlUtil.elements(metadata)) {
			if(xmlElement.getLocalName().equalsIgnoreCase("RecordSource")) {
				String v=XmlUtil.getElementTextByTagNameIgnoreCase(xmlElement, "efg:sourceID");
				if(!StringUtils.isEmpty(v))
					elementsByProperty.put(DcTerms.identifier, cho.getModel().createLiteral(v));
			}else if(xmlElement.getLocalName().equalsIgnoreCase("NonAVManifestation")) {
				processNonAVManifestation(cho, xmlElement, elementsByProperty);
			}else if(xmlElement.getLocalName().equalsIgnoreCase("title")) {
				String v=XmlUtil.getElementTextByTagNameIgnoreCase(xmlElement, "efg:text");
				if(!StringUtils.isEmpty(v))
					elementsByProperty.put(DcTerms.title, cho.getModel().createLiteral(v));
			}else if(xmlElement.getLocalName().equalsIgnoreCase("relPerson")) {
				MappingsContext.processRelAgent(cho, xmlElement, elementsByProperty);
			}else if(xmlElement.getLocalName().equalsIgnoreCase("relCorporate")) {				
				MappingsContext.processRelAgent(cho, xmlElement, elementsByProperty);
			}else if(xmlElement.getLocalName().equalsIgnoreCase("dateCreated")) {				
				String v=XmlUtil.getElementText(xmlElement);
				if(!StringUtils.isEmpty(v))
					elementsByProperty.put(DcTerms.date, cho.getModel().createLiteral(v));				
			}else if(xmlElement.getLocalName().equalsIgnoreCase("keywords")) {	
				String v=XmlUtil.getElementText(xmlElement);
				if(!StringUtils.isEmpty(v))
					elementsByProperty.put(DcTerms.subject, cho.getModel().createLiteral(v));				
			}else if(xmlElement.getLocalName().equalsIgnoreCase("Description")) {	
				String v=XmlUtil.getElementText(xmlElement);
				if(!StringUtils.isEmpty(v))
					elementsByProperty.put(DcTerms.description, cho.getModel().createLiteral(v));				
			}else if(xmlElement.getLocalName().equalsIgnoreCase("identifier")) {	
				String v=XmlUtil.getElementText(xmlElement);
				if(!StringUtils.isEmpty(v))
					elementsByProperty.put(DcTerms.identifier, cho.getModel().createLiteral(v));				
			}else if(xmlElement.getLocalName().equalsIgnoreCase("relAvCreation")) {	
				String v=XmlUtil.getElementTextByTagNameIgnoreCase(xmlElement, "efg:identifier");
				if(!StringUtils.isEmpty(v))
					elementsByProperty.put(DcTerms.relation, cho.getModel().createLiteral(v));			
				v=XmlUtil.getElementTextByTagNameIgnoreCase(xmlElement, "efg:title");
				if(!StringUtils.isEmpty(v))
					elementsByProperty.put(DcTerms.relation, cho.getModel().createLiteral(v));			
			}else if(xmlElement.getLocalName().equalsIgnoreCase("relNonAvCreation")) {				
				String v=XmlUtil.getElementTextByTagNameIgnoreCase(xmlElement, "efg:identifier");
				if(!StringUtils.isEmpty(v))
					elementsByProperty.put(DcTerms.relation, cho.getModel().createLiteral(v));			
				v=XmlUtil.getElementTextByTagNameIgnoreCase(xmlElement, "efg:title");
				if(!StringUtils.isEmpty(v))
					elementsByProperty.put(DcTerms.relation, cho.getModel().createLiteral(v));			
			}else if(xmlElement.getLocalName().equalsIgnoreCase("Item")) {				
				String v=XmlUtil.getElementTextByTagNameIgnoreCase(xmlElement,"efg:isShownBy");
				if(!StringUtils.isEmpty(v)) 
					elementsByProperty.put(DcTerms.identifier, cho.getModel().createLiteral(v));										
				v=XmlUtil.getElementTextByTagNameIgnoreCase(xmlElement,"efg:isShownAt");
				if(!StringUtils.isEmpty(v)) 
					elementsByProperty.put(DcTerms.identifier, cho.getModel().createLiteral(v));	
			}
		}
		
	}

	
	private static void processNonAVManifestation(Resource subject, Element xmlElement, MapOfLists<Property,RDFNode> elementsByProperty) {
		for (Element subXmlElement: XmlUtil.elements(xmlElement)) {
			if(subXmlElement.getLocalName().equalsIgnoreCase("Language")) {
				String v=XmlUtil.getElementText(subXmlElement);
				if(!StringUtils.isEmpty(v))
					elementsByProperty.put(DcTerms.language, subject.getModel().createLiteral(v));
			}else if(subXmlElement.getLocalName().equalsIgnoreCase("specificType")) {
				String v=XmlUtil.getElementText(subXmlElement);
				if(!StringUtils.isEmpty(v))
					elementsByProperty.put(DcTerms.type, subject.getModel().createLiteral(v));
			}else if(subXmlElement.getLocalName().equalsIgnoreCase("geographicScope")) {
				String v=XmlUtil.getElementText(subXmlElement);
				if(!StringUtils.isEmpty(v))
					elementsByProperty.put(DcTerms.coverage, subject.getModel().createLiteral(v));
			}else if(subXmlElement.getLocalName().equalsIgnoreCase("date")) {
				String v=XmlUtil.getElementText(subXmlElement);
				if(!StringUtils.isEmpty(v))
					elementsByProperty.put(DcTerms.date, subject.getModel().createLiteral(v));
			}else if(subXmlElement.getLocalName().equalsIgnoreCase("physicalFormat")
			         || subXmlElement.getLocalName().equalsIgnoreCase("digitalFormat")) {
				String v=XmlUtil.getElementText(subXmlElement);
				if(!StringUtils.isEmpty(v))
					elementsByProperty.put(DcTerms.format, subject.getModel().createLiteral(v));
			}else if(subXmlElement.getLocalName().equalsIgnoreCase("rightsHolder")) {
				String v=XmlUtil.getElementText(subXmlElement);
				if(!StringUtils.isEmpty(v))
					elementsByProperty.put(DcTerms.rights, subject.getModel().createLiteral(v));
			}else if(subXmlElement.getLocalName().equalsIgnoreCase("thumbnail")) {
				String v=XmlUtil.getElementText(subXmlElement);
				if(!StringUtils.isEmpty(v))
					elementsByProperty.put(DcTerms.identifier, subject.getModel().createLiteral(v));
			}
		}
	}
	
}
