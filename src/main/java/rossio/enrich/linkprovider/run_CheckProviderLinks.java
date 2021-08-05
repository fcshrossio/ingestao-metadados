package rossio.enrich.linkprovider;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.output.FileWriterWithEncoding;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.solr.client.solrj.SolrServerException;

import rossio.data.models.DcTerms;
import rossio.data.models.Rossio;
import rossio.export.DatasetExporterInHtml;
import rossio.http.HttpRequest;
import rossio.ingest.solr.RepositoryWithSolr;
import rossio.ingest.solr.RepositoryWithSolr.ItemHandler;
import rossio.ingest.solr.manager.OaiSource;
import rossio.ingest.solr.manager.OaiSources;
import rossio.util.AccessException;
import rossio.util.Global;
import rossio.util.HttpUtil;
import rossio.util.RdfUtil;
import rossio.util.RdfUtil.Jena;

public class run_CheckProviderLinks {
	File exportFolder;
	
	public run_CheckProviderLinks(String exportFolder) {
		super();
		this.exportFolder = new File(exportFolder);
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Global.init_componentHttpRequestService();
		
//    	RepositoryWithSolr repository=new RepositoryWithSolr("http://192.168.111.115:8983/solr/repositorio");
    	RepositoryWithSolr repository=new RepositoryWithSolr("http://192.168.111.170:8983/solr/repositorio");
    	run_CheckProviderLinks enrichTast=new run_CheckProviderLinks("c:/users/nfrei/desktop/ROSSIO_Exports/links");
		
    	OaiSources oaiSources=new OaiSources(new File("src/data/oai_sources-test1.txt"));
    	for(OaiSource source:oaiSources.getAllSources()) {
    		enrichTast.runOnCollection(repository, source.getSourceId());
    	}
    	
	}

	private void runOnCollection(RepositoryWithSolr repository, String sourceId) {
		boolean testing = true;
		ArrayList<LinksReport> reports=new ArrayList<LinksReport>();
		
		try {
			repository.getItemsInSourceVersionAtSource(sourceId, new ItemHandler() {
				int recCount=0;
				@Override
				public boolean handle(String uuid, String idAtSource, String lastUpdate, byte[] content) throws Exception {
					RDFParser reader = RDFParser.create().lang(Lang.RDFTHRIFT).source(new ByteArrayInputStream(content)).build();
					Model model = Jena.createModel();
					reader.parse(model);

					reports.add(runOnRecord(model.createResource(Rossio.NS_ITEM+uuid)));

					recCount++;
					return !testing || recCount<20;
				}
			});

			FileWriterWithEncoding out=new FileWriterWithEncoding(new File(exportFolder ,URLEncoder.encode(sourceId, "UTF-8")+".html"), StandardCharsets.UTF_8);
			out.append("<html><body>\n");

			for(LinksReport rep: reports) {
				out.append("<h3>Registo "+rep.item.getURI()+"</h3>\n");
				DatasetExporterInHtml.writeItem(out, sourceId, rep.item.getModel(), rep.item.getURI());
				if(rep.tests.isEmpty())
					out.append("Sem links\n");
				else {	
					out.append("<table border='1'>\n");
					boolean first=true;
					for(LinkTest t:rep.tests) {
						if(first)
							first=false;
						else
							out.append("<tr><td>&nbsp;</td></tr>\n");
						out.append("<tr><td><a href=\""+t.url+"\">"+t.getUrlForPrint()+ "</a></td></tr>\n");
						out.append("<tr><td>"+t.result+"</td></tr>\n");
					}
					out.append("</table>\n<hr />");
				}
			}
			out.append("</table></body></html>");
			out.close();
			
			

//			FileWriterWithEncoding out=new FileWriterWithEncoding(new File("target/"+URLEncoder.encode(sourceId, "UTF-8")+".html"), StandardCharsets.UTF_8);
//			out.append("<html><body><table border='1'><tr><td><b>Thumbnail</b></td>"
//					+ "<td><b>Link para o provedor</b></td>"
//					+ "<td><b>Links com erros</b></td></tr>\n");
//			for (LinksReport rep: reports) {
//				out.append("<tr><td>");
//				if(rep.thumbnail!=null) {
//					out.append("<a href='"+rep.thumbnail+"'>ver imagem</a>");
//				}
//				out.append("</td><td>");
//				if(rep.isShownAt!=null) {
//					out.append("<a href='"+rep.isShownAt+"'>link</a>");
//				}else {
//					out.append("sem link");					
//				}
//				out.append("</td><td>");
//				if(rep.linkWithError!=null) {
//					out.append("sim");
//				}else {
//					out.append("");					
//				}
//				out.append("</td></tr>");
//			}
//			out.append("</table></body></html>");
//			out.close();
			
		} catch (SolrServerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static LinksReport runOnRecord(Resource choRes) {
		LinksReport rep=new LinksReport();
		rep.item=choRes;
		rep.tests=ChoLinksUtil.testAllLinks(choRes, DcTerms.identifier, DcTerms.relation);
		return rep;
	}

}
