package rossio.test;

import org.apache.commons.lang3.StringUtils;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.rdf.model.Literal;

import rossio.data.models.Rossio;
import rossio.enrich.metadata.RecordEnrichmentGeo;
import rossio.sparql.SparqlClient;
import rossio.util.Handler;

public class TestSparql {

	
	public static void main(String[] args) throws Exception {
//		String vocabsSparqlEndpointUrl="http://192.168.111.170:3030/skosmos/sparql";
		String vocabsSparqlEndpointUrl="http://vocabs.rossio.fcsh.unl.pt:3030/skosmos/sparql";
		
		String q=
		"SELECT ?s ?l WHERE { GRAPH <"+Rossio.NS_LUGARES+"> { "
		+ " {"
		+ " ?s rdf:type bf:Place ; "
		+ " skos:prefLabel ?l ."
		+ " } UNION { "
		+ " ?s rdf:type bf:Place ; "
		+ " skos:altLabel ?l ." 
		+ " } "
		+ "}}";
		
		SparqlClient sparqlRossioVocab = SparqlClient.newInstanceRossio(vocabsSparqlEndpointUrl);
//		sparqlRossioVocab.setDebug(true);
		System.out.println(q);
		sparqlRossioVocab.setDebug(true);
		
		
		sparqlRossioVocab.query(q, new Handler<QuerySolution>() {
			public boolean handle(QuerySolution solution) throws Exception {
				String uri = solution.getResource("s").getURI();
				Literal lableLiteral = solution.getLiteral("l");
				System.out.println(uri);
				
				return true;
			}
		});
		
		
	}
}
