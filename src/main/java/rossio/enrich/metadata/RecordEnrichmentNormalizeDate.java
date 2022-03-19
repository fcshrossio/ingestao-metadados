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
import rossio.sparql.SparqlClient;
import rossio.util.Global;
import rossio.util.Handler;
import rossio.util.MapOfMaps;
import rossio.util.RdfUtil;
import rossio.util.RdfUtil.Jena;

public class RecordEnrichmentNormalizeDate implements RecordEnrichment {
	
	public abstract class DatePattern {
		protected String normalized;
		protected ChronoField precision;
		protected String toNormalize;		
		
		public DatePattern(String toNormalize) {
			this.toNormalize = toNormalize;
		}
		protected abstract void normalize(String toNormalize);
		
		protected void normalizeWithFix(int year, int month, int day, ChronoField precision) {
			try {				
				normalized=formatDateForSolr(ZonedDateTime.of(year, month, day, 0, 0, 0, 0, ZoneId.systemDefault()), precision);
			} catch (Exception e) {
				if (precision==ChronoField.DAY_OF_MONTH) {
					try {				
						normalized=formatDateForSolr(ZonedDateTime.of(year, month, 1, 0, 0, 0, 0, ZoneId.systemDefault()), ChronoField.MONTH_OF_YEAR);
						this.precision=ChronoField.MONTH_OF_YEAR;
					} catch (Exception e2) {
						normalized=formatDateForSolr(ZonedDateTime.of(year, 1, 1, 0, 0, 0, 0, ZoneId.systemDefault()), ChronoField.YEAR);
						this.precision=ChronoField.YEAR;
					}
				}else if (precision==ChronoField.MONTH_OF_YEAR) {
					normalized=formatDateForSolr(ZonedDateTime.of(year, 1, 1, 0, 0, 0, 0, ZoneId.systemDefault()), ChronoField.YEAR);					
					this.precision=ChronoField.YEAR;
				}
			}
		}
		
		
		public String getNormalized() {
			if(normalized==null)
				normalize(toNormalize);
			return normalized;
		}
		public ChronoField getPrecision() {
			return precision;
		}
	}
	public class DatePatternYyyyMmDd extends DatePattern {
		public DatePatternYyyyMmDd(String valueToNormalize) {
			super(valueToNormalize);
		}
		protected void normalize(String valueToNormalize) {
			Pattern pattern=Pattern.compile("(\\d{4})[-/](\\d{1,2})[-/](\\d{1,2})");
			Matcher matcher = pattern.matcher(valueToNormalize);
			if(matcher.matches()) {
				int year=Integer.parseInt(matcher.group(1));
				int month=Integer.parseInt(matcher.group(2));
				int day=Integer.parseInt(matcher.group(3));
				ChronoField precision=ChronoField.DAY_OF_MONTH;
				if(day==0) {
					precision=ChronoField.MONTH_OF_YEAR;
					day=1;
				}
				if(month==0) {
					precision=ChronoField.YEAR;
					month=1;
				}
				if(month>12 && day>0 && day<=12) {
					int tmp=day;
					day=month;
					month=tmp;
				}
				
//					normalized=formatDateForSolr(ZonedDateTime.of(year, month, day, 0, 0, 0, 0, ZoneId.systemDefault()), precision);
				normalizeWithFix(year, month, day, precision);
			}
		}
	}
	public class DatePatternDdMmYyyy extends DatePattern {
		Pattern pattern=Pattern.compile("(\\d{1,2})[-/](\\d{1,2})[-/](\\d{4})");
		public DatePatternDdMmYyyy(String valueToNormalize) {
			super(valueToNormalize);
		}
		protected void normalize(String valueToNormalize) {
			Matcher matcher = pattern.matcher(valueToNormalize);
			if(matcher.matches()) {
				int year=Integer.parseInt(matcher.group(3));
				int month=Integer.parseInt(matcher.group(2));
				int day=Integer.parseInt(matcher.group(1));
				ChronoField precision=ChronoField.DAY_OF_MONTH;
				if(day==0) {
					precision=ChronoField.MONTH_OF_YEAR;
					day=1;
				}
				if(month==0) {
					precision=ChronoField.YEAR;
					month=1;
				}
				if(month>12 && day>0 && day<=12) {
					int tmp=day;
					day=month;
					month=tmp;
				}
					
//				normalized=formatDateForSolr(ZonedDateTime.of(year, month, day, 0, 0, 0, 0, ZoneId.systemDefault()), precision);
				normalizeWithFix(year, month, day, precision);			}
		}
	}

	public class DatePatternYyyyMm extends DatePattern {
		Pattern pattern=Pattern.compile("(\\d{4})[-/](\\d{1,2})");
		public DatePatternYyyyMm(String valueToNormalize) {
			super(valueToNormalize);
		}
		protected void normalize(String valueToNormalize) {
			Matcher matcher = pattern.matcher(valueToNormalize);
			if(matcher.matches()) {
				int year=Integer.parseInt(matcher.group(1));
				int month=Integer.parseInt(matcher.group(2));
				ChronoField precision=ChronoField.MONTH_OF_YEAR;
				if(month==0) {
					precision=ChronoField.YEAR;
					month=1;
				}
//				normalized=formatDateForSolr(ZonedDateTime.of(year, month, 1, 0, 0, 0, 0, ZoneId.systemDefault()), precision);
				normalizeWithFix(year, month, 0, precision);
			}
		}
	}
	public class DatePatternYyyy extends DatePattern {
		Pattern pattern=Pattern.compile("[^\\d]*(\\d{4})[^\\d]*");
//		Pattern pattern=Pattern.compile("[\\[\\s]*(\\d{4})[\\s\\?\\]]*");
		public DatePatternYyyy(String valueToNormalize) {
			super(valueToNormalize);
		}
		protected void normalize(String valueToNormalize) {
			Matcher matcher = pattern.matcher(valueToNormalize);
			if(matcher.matches()) {
				int year=Integer.parseInt(matcher.group(1));
				ChronoField precision=ChronoField.YEAR;
//				normalized=formatDateForSolr(ZonedDateTime.of(year, 1, 1, 0, 0, 0, 0, ZoneId.systemDefault()), precision);
				normalizeWithFix(year, 1, 1, precision);
			}
		}
	}
	
	public RecordEnrichmentNormalizeDate() {
		
	}
	
	protected List<Property> getPropertiesToNormalize() {
		return new ArrayList<Property>() {{
			add(DcTerms.date);
			add(DcTerms.created);
			add(DcTerms.available);
			add(DcTerms.dateAccepted);
			add(DcTerms.dateCopyrighted);
			add(DcTerms.dateSubmitted);
			add(DcTerms.issued);
			add(DcTerms.modified);
			add(DcTerms.temporal);
		}};
	}
	
	
//	protected abstract List<Property> getPropertiesToEnrich();
	
	@Override
	public void enrich(Resource scho) {
		List<Statement> dateProps = new ArrayList<Statement>();
		for(Property prop: getPropertiesToNormalize()) {
			dateProps.addAll(scho.listProperties(prop).toList());			
		}
		Model model = scho.getModel();
		Resource proxy=RdfUtil.getResourceIfExists(scho.getURI()+"#proxy", scho.getModel());

		for(Statement st: dateProps) {
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
		String normalized=null;
		for(DatePattern dtPat: new DatePattern[] {
				new DatePatternDdMmYyyy(label), 
				new DatePatternYyyyMm(label), 
				new DatePatternYyyyMmDd(label), 
				new DatePatternYyyy(label) }) {
				try {
					normalized=dtPat.getNormalized();
				} catch (Exception e) {
					if(Global.DEBUG)
						System.out.println("DEBUG: Error on '"+label+"'\n"+ExceptionUtils.getStackTrace(e));
				}
				if(normalized!=null) break;
			}

		if (normalized!=null) {
//			System.out.println("Normalized: "+label+" "+normalized);
			if(proxy==null) {
				proxy=model.createResource(scho.getURI()+"#proxy", Ore.Proxy);
				proxy.addProperty(Ore.proxyFor, scho);
				proxy.addProperty(Ore.proxyIn, model.createResource(scho.getURI()+"#aggregation", Ore.Aggregation));
			}
			proxy.addProperty(predicate, model.createLiteral(normalized));
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
