package rossio.ingest.preprocess;

import java.util.ArrayList;

import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Seq;
import org.apache.jena.vocabulary.DCTerms;
import org.w3c.dom.Element;

import rossio.data.models.DcTerms;
import rossio.data.models.Rossio;
import rossio.ingest.solr.RossioRecord;
import rossio.util.MapOfLists;
import rossio.util.RdfUtil;
import rossio.util.XmlUtil;

public class ModernismoPreprocessor extends CsvMetadataPreprocessor {
	
	public ModernismoPreprocessor() {
		mapping.put(0, DCTerms.relation);
		mapping.put(1, DCTerms.relation);
		mapping.put(2, DCTerms.identifier);
		mapping.put(3, DCTerms.creator);
		mapping.put(4, DCTerms.title);
		mapping.put(5, DCTerms.title);
		mapping.put(6, DCTerms.description);
		mapping.put(7, DCTerms.language);//Há registos com várias línguas no mesmo campo, separadas por /
		mapping.put(8, DCTerms.description);//Algumas entradas chegam às 1000 palavras. Considerar truncar para mais de 250 caracteres.
		mapping.put(9, DCTerms.subject);//Há registos com várias categorias no mesmo campo, separadas por /
		mapping.put(10, DCTerms.subject);//Há registos com várias subcategorias no mesmo campo, separadas por /
		mapping.put(11, DCTerms.description);
		mapping.put(12, DCTerms.format);
		mapping.put(13, DCTerms.format);
		mapping.put(19, DCTerms.identifier);
		mapping.put(15, DCTerms.identifier);//cota
		mapping.put(16, DCTerms.date);//Vários registos "Sem data". Eliminar a ocorrência do campo nesses casos.
	}

	@Override
	public Model preprocess(String uuid, String sourceId, String dataProviderUri, CSVRecord metadata) {
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
	
	private void processRecord(String uuid, String sourceId, String dataProviderUri, CSVRecord metadata, Resource cho,
			MapOfLists<Property, RDFNode> elementsByProperty) {
		Model m=cho.getModel();
		int idx=-1;
		for(String val: metadata) {
			idx++;
			if(StringUtils.isEmpty(val))
				continue;
			val=val.trim();
			Property propToMap=mapping.get(idx);
			if(propToMap==null) 
				continue;
			if(idx==7 || idx==9 || idx==10) { //Há registos com várias línguas no mesmo campo, separadas por /
				String[] split = val.split("/");
				for(String v: split) {
					if(!StringUtils.isEmpty(v))
						elementsByProperty.put(propToMap, m.createLiteral(v.trim()));
				}
			}else if(idx==8) { //Algumas entradas chegam às 1000 palavras. Considerar truncar para mais de 250 caracteres.
				if(val.length()>255) {
					elementsByProperty.put(propToMap, m.createLiteral(val.substring(0, 250)+"[...]"));
				}else
					elementsByProperty.put(propToMap, m.createLiteral(val));				
			}else if(idx==16) {
				if(!val.equals("Sem data")) 
					elementsByProperty.put(propToMap, m.createLiteral(val));				
			}else if(idx==2) {
				if(val.startsWith("/images")) 
					elementsByProperty.put(propToMap, m.createLiteral("https://modernismo.pt"+val));
				else
					elementsByProperty.put(propToMap, m.createLiteral(val));				
			}else{		
				elementsByProperty.put(propToMap, m.createLiteral(val));
			}
		}
	}

	@Override
	public Model preprocess(String uuid, String sourceId, String dataProviderUri, Element metadata) {
		throw new NotImplementedException();
	}
	
}
