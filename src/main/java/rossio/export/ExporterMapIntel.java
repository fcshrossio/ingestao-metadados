package rossio.export;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFOps;
import org.apache.jena.riot.system.StreamRDFWriter;
import org.apache.solr.client.solrj.SolrServerException;

import rossio.data.models.DcTerms;
import rossio.data.models.Dcat;
import rossio.data.models.Rdf;
import rossio.data.models.Rossio;
import rossio.dspace.DspaceApiClient;
import rossio.dspace.DspaceApiClient.ModelHandler;
import rossio.ingest.solr.RepositoryWithSolr;
import rossio.ingest.solr.RepositoryWithSolr.FetchOption;
import rossio.ingest.solr.RepositoryWithSolr.ItemHandler;
import rossio.ingest.solr.manager.MetadataSource;
import rossio.ingest.solr.manager.MetadataSources;
import rossio.util.RdfUtil;
import rossio.util.RdfUtil.Jena;

public class ExporterMapIntel extends DatasetExporter {
	
	
	public class MetadataToExport {
		String uri;
		Statement titleSt;
		Statement descriptionSt;
		Statement subjectSt;
		boolean insufficientData=true;

		public MetadataToExport(Model model, String choUri) {
			uri=choUri;
			Resource cho = model.createResource(choUri);
			titleSt=cho.getProperty(DcTerms.title);
			if(titleSt!=null) {
				 descriptionSt=cho.getProperty(DcTerms.description);
				if(descriptionSt!=null) {
					 subjectSt=cho.getProperty(DcTerms.subject);
					if(subjectSt!=null) 
						insufficientData=false;
				}
			}
		}
				
	}
	
	MapintelJsonWriter writer;
	int recCountExportedTotal=0;
	
	public ExporterMapIntel(RepositoryWithSolr repository, MetadataSources oaiSources) {
		super(repository, oaiSources);
	}
	
	public void exportMapIntelSample(File homeFolder, int sampleSize) throws IOException, SolrServerException {
		writer=new MapintelJsonWriter(homeFolder);
		for(MetadataSource oaiSource: oaiSources.getAllSources()) {
			exportDatasetMapIntel(homeFolder, oaiSource.getSourceId(), sampleSize);
		}
		writer.close();
	}

	private void exportDatasetMapIntel(File homeFolder, String sourceId, int sampleSize) throws IOException, SolrServerException {
    	//iterate all records and write rdf to bitStream 
    	repository.getItemsInSource(sourceId, FetchOption.VERSION_AT_ROSSIO, new ItemHandler() {
    		int recCount=0;
    		int recCountExported=0;
			@Override
			public boolean handle(String uuid, String idAtSource, String lastUpdate, byte[] contentSource, byte[] content) throws Exception {
				RDFParser reader = RDFParser.create().lang(Lang.RDFTHRIFT).source(new ByteArrayInputStream(content)).build();
				Model model = Jena.createModel();
				reader.parse(model);
				MetadataToExport md=new MetadataToExport(model,  Rossio.NS_ITEM+uuid);
				if(!md.insufficientData) {
					writer.write(md);
					recCountExported++;
					recCountExportedTotal++;
				}
				recCount++;
				return sampleSize<=0 || !(recCountExported>=sampleSize || recCount-recCountExported>sampleSize*10);
			}
		});
	}
	
	
	public static void main(String[] args) throws Exception {
		File exportFolder=new File("c:/users/nfrei/desktop/ROSSIO_Exports");
		MetadataSources oaiSources=new MetadataSources(new File("src/data/oai_sources.ttl"));
//		MetadataSources oaiSources=new MetadataSources(new File("src/data/oai_sources-test-short.ttl"));
//		RepositoryWithSolr repository=new RepositoryWithSolr("http://datarossio.dglab.gov.pt:8983/solr/testes-repositorio");
    	RepositoryWithSolr repository=new RepositoryWithSolr("http://192.168.111.170:8983/solr/repositorio");
		ExporterMapIntel exporter=new ExporterMapIntel(repository, oaiSources);
		exporter.exportMapIntelSample(exportFolder, 200);
		System.out.println(exporter.recCountExportedTotal+" exported records");
	}
	
}
