package rossio.enrich.metadata.stats;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.system.StreamRDFOps;
import org.apache.solr.client.solrj.SolrServerException;

import rossio.data.models.Rossio;
import rossio.ingest.solr.RepositoryWithSolr;
import rossio.ingest.solr.RepositoryWithSolr.ItemHandler;
import rossio.util.MapOfInts;
import rossio.util.RdfUtil;
import rossio.util.RdfUtil.Jena;

public class CreateStatsTask {
	public class PropertyStats {
		MapOfInts<String> valueCounts=new MapOfInts<String>();
		MapOfInts<Integer> occurrencesCounts=new MapOfInts<Integer>();
		
		public void toString(Appendable writer) throws IOException {
			writer.append("Values per record:\n");
			for(Entry<Integer, Integer> e: occurrencesCounts.getSortedEntries()) {
				writer.append(" "+e.getKey()+" - "+e.getValue()+"\n");
			}
			writer.append("Occourrences of values:\n");
			for(Entry<String, Integer> e: valueCounts.getSortedEntries()) {
				writer.append(" \""+e.getKey()+"\" - "+e.getValue()+"\n");
			}
		}

		public void toStringHtml(Appendable writer, String title) throws IOException {
			writer.append("<html><head><title>").append(title).append("</title></head><body>\n");
			writer.append("<h2>").append(title).append("</h2>\n");
			writer.append("<h3>Ocorrências da propriedade por registo:</h3>");
			writer.append("<table border='1'>\n<tr><td><b>Número de ocorrências</b></td><td><b>Número de registos</b></td></tr>\n");
			for(Entry<Integer, Integer> e: occurrencesCounts.getSortedEntries()) {
				writer.append("<tr><td style=\"text-align:center\">"+e.getKey()+"</td><td style=\"text-align:center\">"+e.getValue()+"</td></tr>\n");
			}
			writer.append("</table>\n");
			writer.append("<h3>Ocorrências de valores:</h3>\n");
			writer.append("<table border='1'>\n<tr><th>Valor</th><th>Número de ocorrências</th></tr>\n");
			for(Entry<String, Integer> e: valueCounts.getSortedEntries()) {
				writer.append("<tr><td>"+e.getKey()+"</td><td style=\"text-align:center\">"+e.getValue()+"</td></tr>\n");
			}
			writer.append("</table>\n");
			writer.append("</body></html>\n");
		}
	}

	public boolean testing=false;
	
	
	public CreateStatsTask() {
		super();
	}

	public PropertyStats runOnCollection(RepositoryWithSolr repository, String sourceId, Property propertyToStat) {
		final PropertyStats propStats=new PropertyStats();
    	try {
			repository.getItemsInSource(sourceId, new ItemHandler() {
				int recCount=0;
				@Override
				public boolean handle(String uuid, String idAtSource, String lastUpdate, byte[] content) throws Exception {
					RDFParser reader = RDFParser.create().lang(Lang.RDFTHRIFT).source(new ByteArrayInputStream(content)).build();
					Model model = Jena.createModel();
					reader.parse(model);

					runOnRecord(model.createResource(Rossio.NS_ITEM+uuid), propertyToStat, propStats);

					return !testing || recCount<20;
				}
			});
		} catch (SolrServerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
    	return propStats;
	}

	public void runOnRecord(Resource record, Property prop, PropertyStats propStats) {
		List<Statement> list = record.listProperties(prop).toList();
		int count=0;
		for(Statement st: list) {
			if(st.getObject().isResource()) {
				if(RdfUtil.isSeq(st.getObject().asResource())) {
					for(RDFNode node: RdfUtil.getAsSeq(st.getObject().asResource()).iterator().toList()) {
						String label = RdfUtil.getUriOrLiteralValue(node);
						if(!StringUtils.isEmpty(label)) {
							propStats.valueCounts.incrementTo(label);
							count++;
						}
					}
				}
			} else {
				String label = RdfUtil.getUriOrLiteralValue(st.getObject());
				if(!StringUtils.isEmpty(label)) {
					propStats.valueCounts.incrementTo(label);
					count++;
				}
 			}
		}
		propStats.occurrencesCounts.incrementTo(count);
	}

	public void runOnRecord(Resource record, Map<Property, PropertyStats> result) {
		for (Property p: result.keySet()) {
			runOnRecord(record, p, result.get(p));
		}
	}

	public Map<Property, PropertyStats> runOnCollectionMultiProp(RepositoryWithSolr repository, String sourceId,
			Property... properties) {
		Map<Property, PropertyStats> result=new HashMap<Property, CreateStatsTask.PropertyStats>();
		for (Property p: properties)
			result.put(p, new PropertyStats());

		try {
			repository.getItemsInSource(sourceId, new ItemHandler() {
				int recCount=0;
				@Override
				public boolean handle(String uuid, String idAtSource, String lastUpdate, byte[] content) throws Exception {
					RDFParser reader = RDFParser.create().lang(Lang.RDFTHRIFT).source(new ByteArrayInputStream(content)).build();
					Model model = Jena.createModel();
					reader.parse(model);

					runOnRecord(model.createResource(Rossio.NS_ITEM+uuid), result);

					return !testing || recCount<20;
				}
			});
		} catch (SolrServerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
    	return result;
	}
	
}
