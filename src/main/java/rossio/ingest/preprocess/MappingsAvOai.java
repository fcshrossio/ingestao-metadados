package rossio.ingest.preprocess;

import java.util.ArrayList;

import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Seq;
import org.w3c.dom.Element;

import rossio.data.models.DcTerms;
import rossio.data.models.Rossio;
import rossio.ingest.solr.RossioRecord;
import rossio.util.MapOfLists;
import rossio.util.RdfUtil;
import rossio.util.XmlUtil;

public class MappingsAvOai extends Mappings {


	
	protected static void preprocessAv(String uuid, String sourceId, String dataProviderUri, Element metadata, Resource cho, MapOfLists<Property,RDFNode> elementsByProperty) {
		Model m=cho.getModel();
		processAVCreation(cho, metadata, elementsByProperty);
	}
	
//	public static void preprocessNonAv(EfgPreprocessor preprocessor, String uuid, String sourceId, String dataProviderUri, Element metadata, Resource cho, MapOfLists<Property,RDFNode> elementsByProperty) {
	protected static void processAVManifestation(Resource subject, Element xmlElement, MapOfLists<Property,RDFNode> elementsByProperty) {
		for (Element subXmlElement: XmlUtil.elements(xmlElement)) {
			if(subXmlElement.getLocalName().equalsIgnoreCase("RecordSource")) {
				String v=XmlUtil.getElementTextByTagNameIgnoreCase(subXmlElement, "efg:SourceID");
				if(!StringUtils.isEmpty(v))
					elementsByProperty.put(DcTerms.identifier, subject.getModel().createLiteral(v));
			}else if(subXmlElement.getLocalName().equalsIgnoreCase("Language")) {
				String v=XmlUtil.getElementText(subXmlElement);
				if(!StringUtils.isEmpty(v))
					elementsByProperty.put(DcTerms.language, subject.getModel().createLiteral(v));
			}else if(subXmlElement.getLocalName().equalsIgnoreCase("Dimension")) {
				String v=XmlUtil.getElementText(subXmlElement);
				if(!StringUtils.isEmpty(v)) {
					String unit=XmlUtil.getElementTextByTagNameIgnoreCase(subXmlElement,"efg:Unit");
					if(unit!=null) 
						v=v+" "+unit;
					elementsByProperty.put(DcTerms.format, subject.getModel().createLiteral(v));
				}
			}else if(subXmlElement.getLocalName().equalsIgnoreCase("Duration")) {
				String v=XmlUtil.getElementText(subXmlElement);
				if(!StringUtils.isEmpty(v)) {
					elementsByProperty.put(DcTerms.format, subject.getModel().createLiteral(v));
					String frt=XmlUtil.getElementTextByTagNameIgnoreCase(subXmlElement,"efg:Framerate");
					if(frt!=null) 
						elementsByProperty.put(DcTerms.format, subject.getModel().createLiteral(frt));
				}
			}else if(subXmlElement.getLocalName().equalsIgnoreCase("Format")) {
				for (Element formatSubEl: XmlUtil.elements(subXmlElement)) {
					if(formatSubEl.getLocalName().equalsIgnoreCase("Color")) {
						String v=XmlUtil.getElementText(formatSubEl);
						if(!StringUtils.isEmpty(v)) 
							elementsByProperty.put(DcTerms.format, subject.getModel().createLiteral(v));
					}else if(formatSubEl.getLocalName().equalsIgnoreCase("Gauge")) {
						String v=XmlUtil.getElementText(formatSubEl);
						if(!StringUtils.isEmpty(v)) 
							elementsByProperty.put(DcTerms.format, subject.getModel().createLiteral(v));						
					}else if(formatSubEl.getLocalName().equalsIgnoreCase("Sound")) {
						String v=XmlUtil.getElementText(formatSubEl);
						if(!StringUtils.isEmpty(v)) 
							elementsByProperty.put(DcTerms.format, subject.getModel().createLiteral(v));						
					}else if(formatSubEl.getLocalName().equalsIgnoreCase("AspectRatio")) {
						String v=XmlUtil.getElementText(formatSubEl);
						if(!StringUtils.isEmpty(v)) 
							elementsByProperty.put(DcTerms.format, subject.getModel().createLiteral(v));						
					}
				}
			}else if(subXmlElement.getLocalName().equalsIgnoreCase("RightsHolder")) {
				String v=XmlUtil.getElementText(subXmlElement);
				if(!StringUtils.isEmpty(v)) 
					elementsByProperty.put(DcTerms.rights, subject.getModel().createLiteral(v));						
			}else if(subXmlElement.getLocalName().equalsIgnoreCase("Thumbnail")) {
				String v=XmlUtil.getElementText(subXmlElement);
				if(!StringUtils.isEmpty(v)) 
					elementsByProperty.put(DcTerms.relation, subject.getModel().createLiteral(v));										
			}else if(subXmlElement.getLocalName().equalsIgnoreCase("item")) {
				MappingsContext.processItem(subject, subXmlElement, elementsByProperty);				
			}
		}
	}

	
	protected static void processAVCreation(Resource subject, Element xmlElement, MapOfLists<Property,RDFNode> elementsByProperty) {
		for (Element xmlSubElement: XmlUtil.elements(xmlElement)) {
//			System.out.println(xmlSubElement.getLocalName());
			
			if(xmlSubElement.getLocalName().equalsIgnoreCase("AVManifestation")) {
				processAVManifestation(subject, xmlSubElement, elementsByProperty);
			}else if(xmlSubElement.getLocalName().equalsIgnoreCase("RecordSource")) {
				String v=XmlUtil.getElementTextByTagNameIgnoreCase(xmlElement, "efg:sourceID");
				if(!StringUtils.isEmpty(v))
					elementsByProperty.put(DcTerms.identifier, subject.getModel().createLiteral(v));
			}else if(xmlSubElement.getLocalName().equalsIgnoreCase("Item")) {
				MappingsContext.processItem(subject, xmlSubElement, elementsByProperty);
			}else if(xmlSubElement.getLocalName().equalsIgnoreCase("AVCreation")) {
				processAVCreation(subject, xmlSubElement, elementsByProperty);
			}else if(xmlSubElement.getLocalName().equalsIgnoreCase("Agent")) {
				MappingsContext.processAgent(subject, xmlSubElement, elementsByProperty);
			}else if(xmlSubElement.getLocalName().equalsIgnoreCase("Credits")) {				
				MappingsContext.processCredits(subject, xmlSubElement, elementsByProperty);
			}else if(xmlSubElement.getLocalName().equalsIgnoreCase("relCorporate")) {
				MappingsContext.processRelAgent(subject, xmlSubElement, elementsByProperty);
			}else if(xmlSubElement.getLocalName().equalsIgnoreCase("relPerson")) {
				MappingsContext.processRelAgent(subject, xmlSubElement, elementsByProperty);
			}else if(xmlSubElement.getLocalName().equalsIgnoreCase("relAVCreation")) {
				processRelAVCreation(subject, xmlSubElement, elementsByProperty);
			}else if(xmlSubElement.getLocalName().equalsIgnoreCase("identifier")) {		
				String v=XmlUtil.getElementText(xmlSubElement);
				if(!StringUtils.isEmpty(v)) 
					elementsByProperty.put(DcTerms.identifier, subject.getModel().createLiteral(v));				
			}else if(xmlSubElement.getLocalName().equalsIgnoreCase("sourceID")) {				
				addLiteral(DcTerms.identifier, XmlUtil.getElementText(xmlSubElement),subject, elementsByProperty);
			}else if(xmlSubElement.getLocalName().equalsIgnoreCase("language")) {				
				addLiteral(DcTerms.language, XmlUtil.getElementText(xmlSubElement),subject, elementsByProperty);
			}else if(xmlSubElement.getLocalName().equalsIgnoreCase("keywords")) {				
				String type=xmlSubElement.getAttribute("type");
				if(!StringUtils.isEmpty(type)) {
					if(type.equals("Genre") || type.equals("Category")) {
						addLiteral(DcTerms.type, XmlUtil.getElementTextByTagNameIgnoreCase(xmlSubElement,"efg:term"),subject, elementsByProperty);
					}else if(type.equals("Subject")) {
						addLiteral(DcTerms.subject, XmlUtil.getElementText(xmlSubElement),subject, elementsByProperty);
					}else if(type.equals("Person")) {
						addLiteral(DcTerms.subject, XmlUtil.getElementText(xmlSubElement),subject, elementsByProperty);						
					}
				}
			}else if(xmlSubElement.getLocalName().equalsIgnoreCase("identifyingTitle")) {				
				addLiteral(DcTerms.title, XmlUtil.getElementText(xmlSubElement),subject, elementsByProperty);
			}else if(xmlElement.getLocalName().equalsIgnoreCase("relPerson")) {
				MappingsContext.processRelAgent(subject, xmlElement, elementsByProperty);
			}else if(xmlElement.getLocalName().equalsIgnoreCase("relCorporate")) {				
				MappingsContext.processRelAgent(subject, xmlElement, elementsByProperty);
			}else if(xmlSubElement.getLocalName().equalsIgnoreCase("countryOfReference")) {
				addLiteral(DcTerms.coverage, XmlUtil.getElementText(xmlSubElement),subject, elementsByProperty);
			}else if(xmlSubElement.getLocalName().equalsIgnoreCase("productionYear")) {				
				addLiteral(DcTerms.date, XmlUtil.getElementText(xmlSubElement),subject, elementsByProperty);				
			}else if(xmlSubElement.getLocalName().equalsIgnoreCase("description")) {				
				addLiteral(DcTerms.description, XmlUtil.getElementText(xmlSubElement),subject, elementsByProperty);				
			}
		}
	}
	
	protected static void processRelAVCreation(Resource subject, Element xmlElement, MapOfLists<Property,RDFNode> elementsByProperty) {
		for (Element xmlSubElement: XmlUtil.elements(xmlElement)) {
			if(xmlSubElement.getLocalName().equalsIgnoreCase("identifier")) {
//				addLiteral(DcTerms.relation, XmlUtil.getElementText(xmlSubElement),subject, elementsByProperty);
			}else if(xmlSubElement.getLocalName().equalsIgnoreCase("title")) {
				addLiteral(DcTerms.relation, XmlUtil.getElementText(xmlSubElement),subject, elementsByProperty);
			}
		}
	}
	
}
