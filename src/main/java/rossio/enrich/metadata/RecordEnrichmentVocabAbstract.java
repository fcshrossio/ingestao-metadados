package rossio.enrich.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFList;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.util.iterator.ExtendedIterator;

import rossio.data.models.DcTerms;
import rossio.data.models.Ore;
import rossio.data.models.Rossio;
import rossio.sparql.SparqlClient;
import rossio.util.Handler;
import rossio.util.MapOfMaps;
import rossio.util.RdfUtil;
import rossio.util.RdfUtil.Jena;

public abstract class RecordEnrichmentVocabAbstract implements RecordEnrichment {
	SparqlClient sparqlRossioVocab;
	
	MapOfMaps<String, String, String> langLabelToUri=new MapOfMaps<String, String, String>();
	HashMap<String, String> ambiguosLablesTo=new HashMap<String, String>();
	
	public RecordEnrichmentVocabAbstract(String sparqlEndpointOfVocab, String labelsQuery) {
		this.sparqlRossioVocab=SparqlClient.newInstanceRossio(sparqlEndpointOfVocab);
//		sparqlRossioVocab.setDebug(true);
		sparqlRossioVocab.query(labelsQuery, new Handler<QuerySolution>() {
			public boolean handle(QuerySolution solution) throws Exception {
				String uri = solution.getResource("s").getURI();
				Literal lableLiteral = solution.getLiteral("l");
				String label = labelToMatchingForm(lableLiteral.getString());
				String lang = lableLiteral.getLanguage();
				if(StringUtils.isEmpty(lang))
					lang="pt";
				//for now we just work with pt
				if(!lang.equals("pt"))
					return true;
				
				if(ambiguosLablesTo.containsKey(lang) && ambiguosLablesTo.get(lang).contains(label))
					return true;
				if(langLabelToUri.containsKey(lang, label)) {
					ambiguosLablesTo.put(lang, label);
					langLabelToUri.remove(lang, label);
				} else
					langLabelToUri.put(lang, label, uri);
//				System.out.println(uri+" --- "+label);
				
				return true;
			}
		});
	}
	
	protected abstract List<Property> getPropertiesToEnrich();
	
	@Override
	public void enrich(Resource scho) {
		List<Statement> geoProps = new ArrayList<Statement>();
		for(Property prop: getPropertiesToEnrich()) {
			geoProps.addAll(scho.listProperties(prop).toList());			
		}
		Resource proxy=RdfUtil.getResourceIfExists(scho.getURI()+"#proxy", scho.getModel());

		for(Statement st: geoProps) {
			if(st.getObject().isLiteral()) {
				proxy=enrichValue(scho, proxy, st.getPredicate(), st.getObject().asLiteral());
			} else if(RdfUtil.isSeq(st.getObject().asResource())) {
				for(RDFNode node: RdfUtil.getAsSeq(st.getObject().asResource()).iterator().toList()) {
					if (node.isLiteral())
						proxy=enrichValue(scho, proxy, st.getPredicate(), node.asLiteral());
				}
			}
		}
//		if (proxy!=null )
//			RdfUtil.printOutRdf(proxy.getModel());		
	}
	
	protected Resource enrichValue(Resource scho, Resource proxy, Property predicate, Literal lableLiteral) {
		Model model = scho.getModel();
		String label = labelToMatchingForm(lableLiteral.getString());
		String lang = lableLiteral.getLanguage();
		if(StringUtils.isEmpty(lang))
			lang="pt";
		//for now we just work with pt
		if(lang.equals("pt")) {
			if (langLabelToUri.containsKey(lang, label)) {
	//			System.out.println("Enrich "+label+" "+langLabelToUri.get(lang, label));
				if(proxy==null) {
					proxy=model.createResource(scho.getURI()+"#proxy", Ore.Proxy);
	
					proxy.addProperty(Ore.proxyFor, scho);
					proxy.addProperty(Ore.proxyIn, model.createResource(scho.getURI()+"#aggregation", Ore.Aggregation));
				}
				
				proxy.addProperty(predicate, model.createResource(langLabelToUri.get(lang, label)));
			}
		}
		return proxy;
	}
	
	private String labelToMatchingForm(String label) {
		return label.toLowerCase();
	}
}
