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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
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
import rossio.enrich.metadata.RecordEnrichmentNormalizeDate.DatePattern;
import rossio.enrich.metadata.RecordEnrichmentNormalizeDate.DatePatternDdMmYyyy;
import rossio.enrich.metadata.RecordEnrichmentNormalizeDate.DatePatternYyyy;
import rossio.enrich.metadata.RecordEnrichmentNormalizeDate.DatePatternYyyyMm;
import rossio.enrich.metadata.RecordEnrichmentNormalizeDate.DatePatternYyyyMmDd;
import rossio.enrich.metadata.language.LanguageNormalizationService;
import rossio.enrich.metadata.language.TargetLanguagesVocabulary;
import rossio.sparql.SparqlClient;
import rossio.util.Global;
import rossio.util.Handler;
import rossio.util.MapOfMaps;
import rossio.util.RdfUtil;
import rossio.util.RdfUtil.Jena;

public class RecordEnrichmentNormalizeLanguage implements RecordEnrichment {
	
	LanguageNormalizationService normalizer=new LanguageNormalizationService(TargetLanguagesVocabulary.ISO_639_3);
	
	public RecordEnrichmentNormalizeLanguage() {
		
	}
	
	protected List<Property> getPropertiesToNormalize() {
		return new ArrayList<Property>() {{
			add(DcTerms.language);
		}};
	}
	
	@Override
	public void enrich(Resource scho) {
		List<Statement> langProps = new ArrayList<Statement>();
		for(Property prop: getPropertiesToNormalize()) 
			langProps.addAll(scho.listProperties(prop).toList());			
		Model model = scho.getModel();
		Resource proxy=RdfUtil.getResourceIfExists(scho.getURI()+"#proxy", scho.getModel());

		for(Statement st: langProps) {
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
		String label = lableLiteral.getString().trim();
		List<String> normalizations = normalizer.normalize(label);
		if (normalizations!=null && !normalizations.isEmpty()) {
			String normalized=normalizations.get(0);
			String normalUri="http://lexvo.org/id/iso639-3/"+normalized;
//			System.out.println("Normalized: "+label+" "+normalUri);
			if(proxy==null) {
				proxy=model.createResource(scho.getURI()+"#proxy", Ore.Proxy);
				proxy.addProperty(Ore.proxyFor, scho);
				proxy.addProperty(Ore.proxyIn, model.createResource(scho.getURI()+"#aggregation", Ore.Aggregation));
			}
			proxy.addProperty(predicate, model.createResource(normalUri));
		}
		return proxy;
	}
	
	
	private static String formatDateForSolr(TemporalAccessor time, ChronoField precision) {
		String formated = DateTimeFormatter.ISO_INSTANT.format(time);
		switch (precision) {
		case YEAR:
			return formated.substring(0, formated.indexOf('-', 1)); 
		case MONTH_OF_YEAR:
			return formated.substring(0, formated.indexOf('-', 5)); 
		case DAY_OF_MONTH:
			return formated.substring(0, formated.indexOf('-', 7)); 
		case MINUTE_OF_HOUR:
		case SECOND_OF_MINUTE:
		case MILLI_OF_SECOND:
			return formated; 
		default:
			throw new RuntimeException("Unsupported precision: "+precision.getDisplayName(Locale.ENGLISH));
		}
	}
	
	
}
