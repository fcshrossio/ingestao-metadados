package rossio.enrich.metadata;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;

import rossio.data.models.DcTerms;
import rossio.data.models.Ore;
import rossio.data.models.Rossio;
import rossio.sparql.SparqlClient;
import rossio.util.Global;
import rossio.util.Handler;
import rossio.util.MapOfMaps;
import rossio.util.RdfUtil;
import rossio.util.RdfUtil.Jena;

public class RecordEnrichmentNormalizeType implements RecordEnrichment {
	SparqlClient sparqlRossioVocab;
	
	Map<String, String> mappingToRossioUri=new HashMap<String, String>();
	
	public RecordEnrichmentNormalizeType(String sparqlEndpointOfVocab) {
		this.sparqlRossioVocab=SparqlClient.newInstanceRossio(sparqlEndpointOfVocab);
//		sparqlRossioVocab.setDebug(true);

		String q="SELECT ?s ?o WHERE { GRAPH <http://purl.org/coar/resource_type/> {\r\n"
				+ "?s skos:exactMatch ?o .\r\n"
				+ "}}";
		
		sparqlRossioVocab.query(q, new Handler<QuerySolution>() {
			public boolean handle(QuerySolution solution) throws Exception {
				String rossioUri = solution.getResource("s").getURI();
				String externalUri = solution.getResource("o").getURI();
				
				mappingToRossioUri.put(externalUri, rossioUri);
				
				return true;
			}
		});
	}
	
	protected List<Property> getPropertiesToEnrich() {
		return new ArrayList<Property>() {{
			add(DcTerms.type);
		}};
	}
	
	@Override
	public void enrich(Resource scho) {
		List<Statement> typeProps = new ArrayList<Statement>();
		for(Property prop: getPropertiesToEnrich()) {
			typeProps.addAll(scho.listProperties(prop).toList());			
		}
		Model model = scho.getModel();
		Resource proxy=RdfUtil.getResourceIfExists(scho.getURI()+"#proxy", scho.getModel());
		
		for(Statement st: typeProps) {
			System.out.println(st.getObject());
			if(!st.getObject().isLiteral() && !st.getObject().isURIResource())
				continue;
			String typeVal=RdfUtil.getUriOrLiteralValue(st.getObject());
//			System.out.println(rossioUri);
			String rossioUri=mappingToRossioUri.get(typeVal);
			if(rossioUri==null)
				rossioUri=mappingToRossioUri.get("http://purl.org/info:eu-repo/#semantics/"+typeVal);
			if ( rossioUri!=null) {
//				System.out.println("Enrich "+typeVal+" "+rossioUri);
				if(proxy==null) {
					proxy=model.createResource(scho.getURI()+"#proxy", Ore.Proxy);
					proxy.addProperty(Ore.proxyFor, scho);
					proxy.addProperty(Ore.proxyIn, model.createResource(scho.getURI()+"#aggregation", Ore.Aggregation));
				}
				proxy.addProperty(st.getPredicate(), model.createResource(rossioUri));
			}
		}
		
//		if (proxy!=null )
//			RdfUtil.printOutRdf(proxy.getModel());		
	}
	
	public static void main(String[] args) throws Exception {
		RecordEnrichmentNormalizeType enrich=new RecordEnrichmentNormalizeType("http://192.168.111.170:3030/skosmos/sparql");
	}
	
	
}
