package rossio.ingest.solr;

import java.util.ArrayList;

import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Seq;
import org.w3c.dom.Element;

import rossio.data.models.DcTerms;
import rossio.data.models.Edm;
import rossio.data.models.Ore;
import rossio.data.models.Rdf;
import rossio.data.models.Rossio;
import rossio.util.MapOfLists;
import rossio.util.XmlUtil;
import rossio.util.RdfUtil.Jena;

public class RossioRecord {

	public static Model fromOaidcToRossio(String uuid, String sourceId, String dataProviderUri, Element metadata) {
		Model m=Jena.createModel();
		Resource subject=m.createResource(Rossio.NS_ITEM+uuid);
		subject.addProperty(Rdf.type, Edm.ProvidedCHO);
		Resource subjectAggregation=m.createResource(Rossio.NS_ITEM+uuid+"#aggregation");
		subjectAggregation.addProperty(Rdf.type, Ore.Aggregation);
		subjectAggregation.addProperty(Edm.dataProvider, m.createResource(dataProviderUri));
		subjectAggregation.addProperty(Edm.datasetName, sourceId);
		subjectAggregation.addProperty(Edm.aggregatedCHO, subject);
				
		MapOfLists<String, Element> elementsByProperty=new MapOfLists<String, Element>();
		for (Element xmlElement: XmlUtil.elements(metadata)) {
			String value=XmlUtil.getText(xmlElement);	
			if(!StringUtils.isEmpty(value))
				elementsByProperty.put(DcTerms.NS+xmlElement.getLocalName(), xmlElement);
		}
		
		for (String property: elementsByProperty.keySet()) {
			ArrayList<Element> values = elementsByProperty.get(property);
			if(values.size()>1) {
				Seq seq=m.createSeq();
				for (Element xmlElement: values) {
					String lang=null;
					if(!StringUtils.isEmpty(xmlElement.getAttribute("lang")) || !StringUtils.isEmpty(xmlElement.getAttribute("xml:lang"))) {
						lang=StringUtils.isEmpty(xmlElement.getAttribute("lang")) ? 
								xmlElement.getAttribute("lang") 
								: xmlElement.getAttribute("xml:lang");
					}
					String value=XmlUtil.getText(xmlElement);	
					seq.add(m.createLiteral(value, lang));
				}
				m.add(m.createStatement(subject, m.createProperty(property), seq));
			}else {
				Element xmlElement=values.get(0);
				String lang=null;
				if(!StringUtils.isEmpty(xmlElement.getAttribute("lang")) || !StringUtils.isEmpty(xmlElement.getAttribute("xml:lang"))) {
					lang=StringUtils.isEmpty(xmlElement.getAttribute("lang")) ? 
							xmlElement.getAttribute("lang") 
							: xmlElement.getAttribute("xml:lang");
				}
				String value=XmlUtil.getText(xmlElement);
				m.add(m.createStatement(subject, m.createProperty(property), m.createLiteral(value, lang)));
			}
		}
		return m;
	}
	
	
}
