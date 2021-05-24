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
import rossio.util.RdfUtil;
import rossio.util.RdfUtil.Jena;

public class DatasetExporter {
	RepositoryWithSolr repository;

	public DatasetExporter(RepositoryWithSolr repository) {
		this.repository = repository;
	}
	
	@SuppressWarnings("serial")
	public void exportAllDatasets(File homeFolder, int sampleSize) throws IOException, SolrServerException {
		HashMap<String, String> dspaceCollectionToRepositorySourceId=readCollectionsIdsMap(homeFolder);
		
		for(Entry<String, String> collections: dspaceCollectionToRepositorySourceId.entrySet()) {
			String repositorySourceId=collections.getValue();
//			repositorySourceId=URLEncoder.encode(repositorySourceId, "UTF8");
			File outFile=new File(homeFolder, URLEncoder.encode(repositorySourceId, "UTF8")+".ttl.gz");
			exportDataset(outFile, repositorySourceId, sampleSize);
		}
	}

	public void exportDataset(File outFile, String sourceId, int sampleSize) throws IOException, SolrServerException {
    	FileOutputStream fos=new FileOutputStream(outFile);
		GZIPOutputStream outStream=new GZIPOutputStream(fos);
		final StreamRDF writer = StreamRDFWriter.getWriterStream(outStream, Lang.TURTLE) ;
		
    	//iterate all records and write rdf to bitStream 
    	repository.getItemsInSource(sourceId, new ItemHandler() {
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
	
	
	private HashMap<String, String> readCollectionsIdsMap(File homeFolder) throws IOException {
		HashMap<String, String> map=new HashMap<String, String>();
		List<String> collections = FileUtils.readLines(new File(homeFolder, "dspaceCollectionToRepositorySourceIdMap.txt"), StandardCharsets.UTF_8);
		for(String collectionLine: collections) {
			if(StringUtils.isEmpty(collectionLine)) continue;
			int idxTab=collectionLine.indexOf('\t');
			if(idxTab<=0)
				throw new IOException("dspaceCollectionToRepositorySourceIdMap.txt has invalid line: "+collectionLine);
			map.put(collectionLine.substring(0, idxTab), collectionLine.substring(idxTab+1));
		}
		return map;
	}
	
	
	public static void main(String[] args) throws Exception {
    	RepositoryWithSolr repository=new RepositoryWithSolr("http://192.168.111.115:8984/solr/repositorio");
    	File outFolder=new File("src/data/test");
    	
    	DatasetExporter exporter=new DatasetExporter(repository);
    	
    	exporter.exportAllDatasets(outFolder, 100);
	}
	
}
