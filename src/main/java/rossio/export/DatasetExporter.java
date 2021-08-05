package rossio.export;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
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
import rossio.ingest.datasets.dspace.DcMetadata;
import rossio.ingest.datasets.dspace.DcatToDctermsConverter;
import rossio.ingest.datasets.dspace.SimpleArchive;
import rossio.ingest.solr.RepositoryWithSolr;
import rossio.ingest.solr.RepositoryWithSolr.ItemHandler;
import rossio.ingest.solr.manager.OaiSource;
import rossio.ingest.solr.manager.OaiSources;
import rossio.util.RdfUtil;
import rossio.util.RdfUtil.Jena;

public class DatasetExporter {
	RepositoryWithSolr repository;
	 OaiSources oaiSources;

	public DatasetExporter(RepositoryWithSolr repository, OaiSources oaiSources) {
		this.repository = repository;
		this.oaiSources = oaiSources;
	}
	
	@SuppressWarnings("serial")
	public void exportAllDatasets(File homeFolder, int sampleSize) throws IOException, SolrServerException {
		
		for(OaiSource oaiSource: oaiSources.getAllSources()) {
			exportDataset(homeFolder, oaiSource.getSourceId(), sampleSize);
		}
	}

	public void exportDataset(File homeFolder, String sourceId, int sampleSize) throws IOException, SolrServerException {
		File outFile=new File(homeFolder, URLEncoder.encode(sourceId, "UTF8")+".ttl.gz");

		FileOutputStream fos=new FileOutputStream(outFile);
		GZIPOutputStream outStream=new GZIPOutputStream(fos);
		final StreamRDF writer = StreamRDFWriter.getWriterStream(outStream, Lang.TURTLE) ;
		
    	//iterate all records and write rdf to bitStream 
    	repository.getItemsInSourceVersionRossio(sourceId, new ItemHandler() {
    		int recCount=0;
			@Override
			public boolean handle(String uuid, String idAtSource, String lastUpdate, byte[] content) throws Exception {
				RDFParser reader = RDFParser.create().lang(Lang.RDFTHRIFT).source(new ByteArrayInputStream(content)).build();
				Model model = Jena.createModel();
				reader.parse(model);
				StreamRDFOps.graphToStream(model.getGraph(), writer) ;
				recCount++;
				return recCount<sampleSize;
			}
		});
    	outStream.close();
    	fos.close();
	}
	
}
