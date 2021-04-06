package rossio.enrich.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
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
import rossio.util.Handler;
import rossio.util.MapOfMaps;
import rossio.util.RdfUtil;
import rossio.util.RdfUtil.Jena;

public class RecordEnrichmentTemporal extends RecordEnrichmentVocabAbstract {
	
	public RecordEnrichmentTemporal(String sparqlEndpointOfVocab) {
		super(sparqlEndpointOfVocab, "SELECT ?s ?l WHERE { GRAPH <"+Rossio.NS_PERIODOS+"> { "
				+ " {"
				+ " ?s rdf:type bf:Temporal ; "
				+ " skos:prefLabel ?l ."
				+ " } UNION { "
				+ " ?s rdf:type bf:Temporal ; "
				+ " skos:altLabel ?l ." 
				+ " } "
				+ "}}");
	}
	
	@Override
	protected List<Property> getPropertiesToEnrich() {
		return new ArrayList<Property>() {{
			add(DcTerms.subject);
			add(DcTerms.temporal);
			add(DcTerms.coverage);
		}};
	}
}
