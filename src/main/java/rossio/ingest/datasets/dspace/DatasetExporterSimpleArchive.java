package rossio.ingest.datasets.dspace;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
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

import rossio.data.models.Dcat;
import rossio.data.models.Rdf;
import rossio.data.models.Rossio;
import rossio.ingest.solr.RepositoryWithSolr;
import rossio.ingest.solr.RepositoryWithSolr.ItemHandler;
import rossio.util.RdfUtil;
import rossio.util.RdfUtil.Jena;

public class DatasetExporterSimpleArchive {
	RepositoryWithSolr repository;

	public DatasetExporterSimpleArchive(RepositoryWithSolr repository) {
		this.repository = repository;
	}

	@SuppressWarnings("serial")
	public void exportAllDatasets(File homeFolder) throws IOException, SolrServerException {
		HashMap<String, String> dspaceCollectionToRepositorySourceId=readCollectionsIdsMap(homeFolder);
		
		for(File dcatRdfFile: homeFolder.listFiles()) {
			if(!dcatRdfFile.getName().endsWith(".dcat.ttl")) 
				continue;
			
			Model dcat = RdfUtil.readRdf(dcatRdfFile, Lang.TURTLE);
			
			//Extract the DSpace collection ID from the URI
			Resource datasetRes = RdfUtil.findFirstResourceWithProperties(dcat, Rdf.type, Dcat.Dataset, null, null);
			String dSpaceCollection=datasetRes.getURI().substring(Rossio.NS_CONJUNTO_DE_DADOS.length()+1);
			String repositorySourceId=dspaceCollectionToRepositorySourceId.get(dSpaceCollection);
			repositorySourceId=URLEncoder.encode(repositorySourceId, "UTF8");
			
	    	File simpleArchiveZip=new File(homeFolder, URLEncoder.encode(dSpaceCollection, "UTF8")+"_simple-archive.zip");
	    	
	    	DcMetadata dcMetadata = DcatToDctermsConverter.convert(dcat);
	    	
	    	SimpleArchive archive=new SimpleArchive(simpleArchiveZip);
	    	archive.addItem(repositorySourceId, dcMetadata);
	    	archive.setCollections(repositorySourceId, dSpaceCollection);
	    	archive.setContents(repositorySourceId, new HashMap<String, String>(){{
	    		put("metadados_dcat.ttl","Metadados sobre o conjunto de dados (no modelo Data Catalog Vocabulary - DCAT versão 2)");
	    		put("conjunto_de_dados_completo.ttl.gz", "Arquivo contendo todo o conjunto de dados (RDF serializado no formato TURTLE)");
	    	}});
	    	archive.addBitStream(repositorySourceId, "metadados_dcat.ttl", RdfUtil.writeRdf(dcat, Lang.TURTLE));
	    	
	    	OutputStream bitStream = archive.openBitStream(repositorySourceId, "conjunto_de_dados_completo.ttl.gz");
			GZIPOutputStream outStream=new GZIPOutputStream(bitStream);
			final StreamRDF writer = StreamRDFWriter.getWriterStream(outStream, Lang.TURTLE) ;
			
	    	//iterate all records and write rdf to bitStream 
	    	repository.getItemsInSource(repositorySourceId, new ItemHandler() {
				@Override
				public boolean handle(String uuid, byte[] content) throws Exception {
					RDFParser reader = RDFParser.create().lang(Lang.RDFTHRIFT).source(new ByteArrayInputStream(content)).build();
					Model model = Jena.createModel();
					reader.parse(model);
					StreamRDFOps.graphToStream(model.getGraph(), writer) ;
					return true;
				}
			});

			
	    	archive.close();
		}
		
		
		
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
	
}
