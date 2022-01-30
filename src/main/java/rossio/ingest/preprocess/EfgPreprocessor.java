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

public class EfgPreprocessor implements MetadataPreprocessor {

	@Override
	public Model preprocess(String uuid, String sourceId, String dataProviderUri, Element metadata) {
		Model m=RossioRecord.createBaseRecord(uuid, sourceId, dataProviderUri);		
		Resource cho=m.createResource(Rossio.NS_ITEM+uuid);
		MapOfLists<Property, RDFNode> elementsByProperty=new MapOfLists<Property, RDFNode>();
		
		boolean nonAvCreation=metadata.getLocalName().equalsIgnoreCase("nonavcreation");
		if(nonAvCreation)
			preprocessNonAv(uuid, sourceId,dataProviderUri,metadata,cho, elementsByProperty);
		else
			preprocessAv(uuid, sourceId,dataProviderUri,metadata,cho, elementsByProperty);
		
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
	
	public void preprocessNonAv(String uuid, String sourceId, String dataProviderUri, Element metadata, Resource cho, MapOfLists<Property,RDFNode> elementsByProperty) {
		Model m=cho.getModel();
		for (Element xmlElement: XmlUtil.elements(metadata)) {
			if(xmlElement.getLocalName().equalsIgnoreCase("RecordSource")) {
				String v=XmlUtil.getElementTextByTagName(xmlElement, "efg:sourceID");
				if(!StringUtils.isEmpty(v))
					elementsByProperty.put(DcTerms.identifier, cho.getModel().createLiteral(v));
			}else if(xmlElement.getLocalName().equalsIgnoreCase("NonAVManifestation")) {
				processNonAVManifestation(cho, xmlElement, elementsByProperty);
			}else if(xmlElement.getLocalName().equalsIgnoreCase("title")) {
				String v=XmlUtil.getElementTextByTagName(xmlElement, "efg:text");
				if(!StringUtils.isEmpty(v))
					elementsByProperty.put(DcTerms.title, cho.getModel().createLiteral(v));
			}else if(xmlElement.getLocalName().equalsIgnoreCase("relPerson")) {
				processRelAgent(cho, xmlElement, elementsByProperty);
			}else if(xmlElement.getLocalName().equalsIgnoreCase("relCorporate")) {				
				processRelAgent(cho, xmlElement, elementsByProperty);
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
				String v=XmlUtil.getElementTextByTagName(xmlElement, "efg:identifier");
				if(!StringUtils.isEmpty(v))
					elementsByProperty.put(DcTerms.relation, cho.getModel().createLiteral(v));			
				v=XmlUtil.getElementTextByTagName(xmlElement, "efg:title");
				if(!StringUtils.isEmpty(v))
					elementsByProperty.put(DcTerms.relation, cho.getModel().createLiteral(v));			
			}else if(xmlElement.getLocalName().equalsIgnoreCase("relNonAvCreation")) {				
				String v=XmlUtil.getElementTextByTagName(xmlElement, "efg:identifier");
				if(!StringUtils.isEmpty(v))
					elementsByProperty.put(DcTerms.relation, cho.getModel().createLiteral(v));			
				v=XmlUtil.getElementTextByTagName(xmlElement, "efg:title");
				if(!StringUtils.isEmpty(v))
					elementsByProperty.put(DcTerms.relation, cho.getModel().createLiteral(v));			
			}else if(xmlElement.getLocalName().equalsIgnoreCase("Item")) {				
				String v=XmlUtil.getElementTextByTagName(xmlElement,"efg:isShownBy");
				if(!StringUtils.isEmpty(v)) 
					elementsByProperty.put(DcTerms.identifier, cho.getModel().createLiteral(v));										
				v=XmlUtil.getElementTextByTagName(xmlElement,"efg:isShownAt");
				if(!StringUtils.isEmpty(v)) 
					elementsByProperty.put(DcTerms.identifier, cho.getModel().createLiteral(v));	
			}
		}
		
	}
	
	public void preprocessAv(String uuid, String sourceId, String dataProviderUri, Element metadata, Resource cho, MapOfLists<Property,RDFNode> elementsByProperty) {
		Model m=cho.getModel();
		processAVCreation(cho, metadata, elementsByProperty);
	}


	
	


	private void processAVManifestation(Resource subject, Element xmlElement, MapOfLists<Property,RDFNode> elementsByProperty) {
		for (Element subXmlElement: XmlUtil.elements(xmlElement)) {
			if(subXmlElement.getLocalName().equalsIgnoreCase("RecordSource")) {
				String v=XmlUtil.getElementTextByTagName(subXmlElement, "efg:SourceID");
				if(!StringUtils.isEmpty(v))
					elementsByProperty.put(DcTerms.identifier, subject.getModel().createLiteral(v));
			}else if(subXmlElement.getLocalName().equalsIgnoreCase("Language")) {
				String v=XmlUtil.getElementText(subXmlElement);
				if(!StringUtils.isEmpty(v))
					elementsByProperty.put(DcTerms.language, subject.getModel().createLiteral(v));
			}else if(subXmlElement.getLocalName().equalsIgnoreCase("Dimension")) {
				String v=XmlUtil.getElementText(subXmlElement);
				if(!StringUtils.isEmpty(v)) {
					String unit=XmlUtil.getElementTextByTagName(subXmlElement,"efg:Unit");
					if(unit!=null) 
						v=v+" "+unit;
					elementsByProperty.put(DcTerms.format, subject.getModel().createLiteral(v));
				}
			}else if(subXmlElement.getLocalName().equalsIgnoreCase("Duration")) {
				String v=XmlUtil.getElementText(subXmlElement);
				if(!StringUtils.isEmpty(v)) {
					elementsByProperty.put(DcTerms.format, subject.getModel().createLiteral(v));
					String frt=XmlUtil.getElementTextByTagName(subXmlElement,"efg:Framerate");
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
			}
		}
	}
	private void processNonAVManifestation(Resource subject, Element xmlElement, MapOfLists<Property,RDFNode> elementsByProperty) {
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

	private void processItem(Resource subject, Element xmlElement, MapOfLists<Property,RDFNode> elementsByProperty) {
		String v=XmlUtil.getElementTextByTagName(xmlElement,"efg:Type");
		if(!StringUtils.isEmpty(v)) 
			elementsByProperty.put(DcTerms.type, subject.getModel().createLiteral(v));										
		v=XmlUtil.getElementTextByTagName(xmlElement,"efg:URI");
		if(!StringUtils.isEmpty(v)) 
			elementsByProperty.put(DcTerms.identifier, subject.getModel().createLiteral(v));										
	}
	
	private void processAVCreation(Resource subject, Element xmlElement, MapOfLists<Property,RDFNode> elementsByProperty) {
		for (Element xmlSubElement: XmlUtil.elements(xmlElement)) {
			System.out.println(xmlSubElement.getLocalName());
			
			if(xmlSubElement.getLocalName().equalsIgnoreCase("AVManifestation")) {
				processAVManifestation(subject, xmlSubElement, elementsByProperty);
			}else if(xmlSubElement.getLocalName().equalsIgnoreCase("Item")) {
				processItem(subject, xmlSubElement, elementsByProperty);
			}else if(xmlSubElement.getLocalName().equalsIgnoreCase("AVCreation")) {
				processAVCreation(subject, xmlSubElement, elementsByProperty);
			}else if(xmlSubElement.getLocalName().equalsIgnoreCase("Agent")) {
				processAgent(subject, xmlSubElement, elementsByProperty);
			}else if(xmlSubElement.getLocalName().equalsIgnoreCase("Credits")) {				
				processCredits(subject, xmlSubElement, elementsByProperty);
			}else if(xmlSubElement.getLocalName().equalsIgnoreCase("relCorporate")) {
				processRelAgent(subject, xmlSubElement, elementsByProperty);
			}else if(xmlSubElement.getLocalName().equalsIgnoreCase("relPerson")) {
				processRelAgent(subject, xmlSubElement, elementsByProperty);
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
					if(type.equals("Genre")) {
						addLiteral(DcTerms.language, XmlUtil.getElementText(xmlSubElement),subject, elementsByProperty);
					}else if(type.equals("Category")) {
						addLiteral(DcTerms.language, XmlUtil.getElementText(xmlSubElement),subject, elementsByProperty);
					}else if(type.equals("Subject")) {
						addLiteral(DcTerms.subject, XmlUtil.getElementText(xmlSubElement),subject, elementsByProperty);
					}else if(type.equals("Person")) {
						addLiteral(DcTerms.subject, XmlUtil.getElementText(xmlSubElement),subject, elementsByProperty);						
					}
				}
			}else if(xmlSubElement.getLocalName().equalsIgnoreCase("identifyingTitle")) {				
				addLiteral(DcTerms.title, XmlUtil.getElementText(xmlSubElement),subject, elementsByProperty);
			}else if(xmlElement.getLocalName().equalsIgnoreCase("relPerson")) {
				processRelAgent(subject, xmlElement, elementsByProperty);
			}else if(xmlElement.getLocalName().equalsIgnoreCase("relCorporate")) {				
				processRelAgent(subject, xmlElement, elementsByProperty);
			}else if(xmlSubElement.getLocalName().equalsIgnoreCase("countryOfReference")) {
				addLiteral(DcTerms.coverage, XmlUtil.getElementText(xmlSubElement),subject, elementsByProperty);
			}else if(xmlSubElement.getLocalName().equalsIgnoreCase("productionYear")) {				
				addLiteral(DcTerms.date, XmlUtil.getElementText(xmlSubElement),subject, elementsByProperty);				
			}else if(xmlSubElement.getLocalName().equalsIgnoreCase("description")) {				
				addLiteral(DcTerms.description, XmlUtil.getElementText(xmlSubElement),subject, elementsByProperty);				
			}
		}
	}
	
	private void processRelAVCreation(Resource subject, Element xmlElement, MapOfLists<Property,RDFNode> elementsByProperty) {
		for (Element xmlSubElement: XmlUtil.elements(xmlElement)) {
			if(xmlSubElement.getLocalName().equalsIgnoreCase("identifier")) {
//				addLiteral(DcTerms.relation, XmlUtil.getElementText(xmlSubElement),subject, elementsByProperty);
			}else if(xmlSubElement.getLocalName().equalsIgnoreCase("title")) {
				addLiteral(DcTerms.relation, XmlUtil.getElementText(xmlSubElement),subject, elementsByProperty);
			}
		}
	}
	
	private void addLiteral(Property prop, String elementText, Resource subject, MapOfLists<Property, RDFNode> elementsByProperty) {
		if(!StringUtils.isEmpty(elementText)) 
			elementsByProperty.put(prop, subject.getModel().createLiteral(elementText));
	}

	private void processAgent(Resource subject, Element xmlElement, MapOfLists<Property,RDFNode> elementsByProperty) {
		for (Element subXmlElement: XmlUtil.elements(xmlElement)) {
			if(subXmlElement.getLocalName().equalsIgnoreCase("Person") || subXmlElement.getLocalName().equalsIgnoreCase("CorporateBody")) {
				String v=XmlUtil.getElementTextByTagName(xmlElement,"efg:Name");
				if(!StringUtils.isEmpty(v)) {
					String role=XmlUtil.getElementTextByTagName(xmlElement,"efg:TypeOfActivity");
					if(!StringUtils.isEmpty(role) && (role.equals("Realizador") || role.equals("Realização")))
						elementsByProperty.put(DcTerms.creator, subject.getModel().createLiteral(v));
					else
						elementsByProperty.put(DcTerms.contributor, subject.getModel().createLiteral(v));
				}
			}
		}
	}
	private void processRelAgent(Resource subject, Element xmlElement, MapOfLists<Property,RDFNode> elementsByProperty) {
		String v=XmlUtil.getElementTextByTagName(xmlElement,"efg:name");
		if(!StringUtils.isEmpty(v)) {
			String role=XmlUtil.getElementTextByTagName(xmlElement,"efg:type");
			if(!StringUtils.isEmpty(role) && (role.equals("Autor") || role.equals("Realizador") || role.equals("Realização")))
				elementsByProperty.put(DcTerms.creator, subject.getModel().createLiteral(v));
			else
				elementsByProperty.put(DcTerms.contributor, subject.getModel().createLiteral(v));
		}
	}

	private void processCredits(Resource subject, Element xmlElement, MapOfLists<Property,RDFNode> elementsByProperty) {
		for (Element subXmlElement: XmlUtil.elements(xmlElement)) {
			if(subXmlElement.getLocalName().equalsIgnoreCase("Person") || subXmlElement.getLocalName().equalsIgnoreCase("CorporateBody")) {
				String v=XmlUtil.getElementTextByTagName(xmlElement,"efg:Name");
				if(!StringUtils.isEmpty(v)) {
					String role=XmlUtil.getElementTextByTagName(xmlElement,"efg:TypeOfActivity");
					if(!StringUtils.isEmpty(role) && role.equals("Distribuidor"))
						elementsByProperty.put(DcTerms.publisher, subject.getModel().createLiteral(v));
					else
						elementsByProperty.put(DcTerms.contributor, subject.getModel().createLiteral(v));
				}
			}
		}
	}
}
