package rossio.export;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.output.FileWriterWithEncoding;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.NodeIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Seq;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFOps;
import org.apache.jena.riot.system.StreamRDFWriter;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.jsoup.Jsoup;

import com.afrozaar.wordpress.wpapi.v2.model.User;

import rossio.data.models.DcTerms;
import rossio.data.models.Edm;
import rossio.data.models.Ore;
import rossio.data.models.Rdf;
import rossio.data.models.Rossio;
import rossio.ingest.solr.RepositoryWithSolr;
import rossio.ingest.solr.RepositoryWithSolr.FetchOption;
import rossio.ingest.solr.RepositoryWithSolr.ItemHandler;
import rossio.util.RdfUtil;
import rossio.util.RdfUtil.Jena;

public class DatasetExporterInHtml {
	RepositoryWithSolr repository;

	public DatasetExporterInHtml(RepositoryWithSolr repository) {
		this.repository = repository;
	}
	
	public void exportDataset(File outFile, String sourceId, int sampleSize, boolean versionAtSource, boolean showAggregationAndProxyb) throws IOException, SolrServerException {
		final FileWriterWithEncoding fileWriter = new FileWriterWithEncoding(outFile, StandardCharsets.UTF_8);
		BufferedWriter writer=new BufferedWriter(fileWriter);
		writer.append("<html><body>");
		
		final FetchOption fetchOption;
		if(versionAtSource)
			fetchOption=FetchOption.VERSION_AT_SOURCE;
		else
			fetchOption=FetchOption.VERSION_AT_ROSSIO;
		
    	//iterate all records and write rdf to bitStream 
		ItemHandler handler=new ItemHandler() {
    		int recCount=0;
			@Override
			public boolean handle(String uuid, String idAtSource, String lastUpdate, byte[] contentSource, byte[] contentRossio) throws Exception {
				byte[] content= fetchOption==FetchOption.VERSION_AT_SOURCE ? contentSource : contentRossio;
				RDFParser reader = RDFParser.create().lang(Lang.RDFTHRIFT).source(new ByteArrayInputStream(content)).build();
				Model model = Jena.createModel();
				reader.parse(model);
				writeItem(writer, sourceId, model, Rossio.NS_ITEM+uuid, showAggregationAndProxyb);
				recCount++;
				return sampleSize<=0 || recCount<sampleSize;
			}
		};
			repository.getItemsInSource(sourceId, fetchOption, handler);
		writer.append("</body></html>");
    	writer.close();
	}
	
	public static void writeItem(Appendable writer, String source, Model model, String providedChoUri, boolean showAggregationAndProxy) throws SolrServerException, IOException {
		Resource cho = model.createResource(providedChoUri);
		Resource aggregation = model.createResource(providedChoUri+"#aggregation");
		Resource proxy = model.createResource(providedChoUri+"#proxy");
		
//		String itemId = providedChoUri.substring(providedChoUri.lastIndexOf('/'));
		writer.append("<table border='1' cellspacing='0' cellpadding='0'>");
		
		writer.append("<tr><td valign='top'><b>Object</b></td><td></td></tr>");
		for(Statement st:cho.listProperties().toList()) {
			if(st.getPredicate().getNameSpace().equals(DcTerms.NS)) {
				writeStatement(st, writer);
			}
		}
		if(showAggregationAndProxy) {
			if(aggregation!=null) {
				List<Statement> aggProps = aggregation.listProperties().toList();
				if(!aggProps.isEmpty()) {
					writer.append("<tr><td valign='top'><b>Aggregation</b></td><td></td></tr>");
					for(Statement st:aggProps) {
						if(!st.getPredicate().getNameSpace().equals(Rdf.NS) && 
								!st.getPredicate().equals(Edm.aggregatedCHO)) 
							writeStatement(st, writer);
					}
				}
			}
			if(proxy!=null) {
				List<Statement> proxyProps = proxy.listProperties().toList();
				if(!proxyProps.isEmpty()) {
					writer.append("<tr><td valign='top'><b>Proxy</b></td><td></td></tr>");
					for(Statement st:proxyProps) {
						if(!st.getPredicate().getNameSpace().equals(Rdf.NS) &&
								!st.getPredicate().getNameSpace().equals(Ore.NS) ) 
							writeStatement(st, writer);
					}
				}
			}
		}
		writer.append("</table><br /><br />");
	}

	private static void writeStatement(Statement st, Appendable writer) throws IOException {
		if (st.getObject().isLiteral()) {
			writer.append("<tr><td valign='top'>"+st.getPredicate().getLocalName()+"</td><td>"+st.getObject().asLiteral().getValue()+"</td></tr>");
		} else if(st.getObject().isResource() && RdfUtil.isSeq(st.getObject().asResource())) {
			Seq seq = RdfUtil.getAsSeq(st.getObject().asResource());
			NodeIterator iter2 = seq.iterator();
			boolean first=true;
		    while (iter2.hasNext()) {
		    	Literal litValue = iter2.next().asLiteral();
//		    	if(first) 
//		    		first=false;
//		    	else
//		    		writer.append("<br />");
		    	writer.append("<tr><td valign='top'>"+st.getPredicate().getLocalName()+"</td><td>");
				writer.append(litValue.getValue().toString());
				writer.append("</td></tr>");
		    }
		} else if(st.getObject().isResource()) {
			writer.append("<tr><td valign='top'>"+st.getPredicate().getLocalName()+"</td><td>");
			writer.append(st.getObject().asResource().getURI());
			writer.append("</td></tr>");			
		}

	}
}
