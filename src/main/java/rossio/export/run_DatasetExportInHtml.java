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
import rossio.ingest.solr.manager.MetadataSource;
import rossio.ingest.solr.manager.MetadataSources;
import rossio.util.RdfUtil;
import rossio.util.RdfUtil.Jena;

public class run_DatasetExportInHtml {
	
	
	public static void main(String[] args) throws Exception {
		File exportFolder=new File("c:/users/nfrei/desktop/ROSSIO_Exports");
			
//http://vocabs.rossio.fcsh.unl.pt/agentes/c_f99bcabf|https://medievalista.iem.fcsh.unl.pt/index.php/medievalista/oai|medievalista|oai_dc|SUCCESS|SUCCESS|
//http://vocabs.rossio.fcsh.unl.pt/agentes/c_5fdab3ef|https://www.fcsh.unl.pt/rcl/index.php/rcl/oai|rcl|oai_dc|SUCCESS|SUCCESS|
//http://vocabs.rossio.fcsh.unl.pt/agentes/c_5fdab3ef|https://impactum-journals.uc.pt/mj/oai|mj|oai_dc|SUCCESS|SUCCESS|
		
//http://vocabs.rossio.fcsh.unl.pt/agentes/c_9becc4f7|https://revistas.rcaap.pt/aham/oai|aham|oai_dc|SUCCESS|OUTDATED|
//http://vocabs.rossio.fcsh.unl.pt/agentes/c_9d49ab59|http://ojs.letras.up.pt/index.php/tm/oai|tm|oai_dc|SUCCESS|OUTDATED|
//http://vocabs.rossio.fcsh.unl.pt/agentes/c_9becc4f7|http://oai.openedition.org/|journals:cultura|oai_dc|SUCCESS|OUTDATED|
//http://vocabs.rossio.fcsh.unl.pt/agentes/c_0ef7b7e5|http://oai.openedition.org/|journals:sociologico|oai_dc|SUCCESS|OUTDATED|
//http://vocabs.rossio.fcsh.unl.pt/agentes/c_fe8a7b7e|http://oai.openedition.org/|journals:etnografica|oai_dc|SUCCESS|OUTDATED|
		
//		String sourceId="https://impactum-journals.uc.pt/mj/oai#mj";
//		String sourceId="https://impactum-journals.uc.pt/mj/oai#mj:ART";
//		String sourceId="https://www.fcsh.unl.pt/rcl/index.php/rcl/oai#rcl";
//		String sourceId="https://medievalista.iem.fcsh.unl.pt/index.php/medievalista/oai#medievalista";
		
//		for(String sourceId: getAllSourceIds()	
				for(String sourceId: 	
			new String[] {
//			"https://projetos.dhlab.fcsh.unl.pt/oai#sociedadedasnacoes",	
//			"https://projetos.dhlab.fcsh.unl.pt/oai#memoriacovid",	
//			"https://projetos.dhlab.fcsh.unl.pt/oai#ulmeiro50anos",	
//			"https://projetos.dhlab.fcsh.unl.pt/oai#GTComenta",	
//			"https://projetos.dhlab.fcsh.unl.pt/oai#ulmeiro50anos_en",	
			"https://projetos.dhlab.fcsh.unl.pt/oai#wsdroadmap",	
//			"https://vvpducalmus.fcsh.unl.pt/fontes/oai#",	
//			"https://arouca.fcsh.unl.pt/fontes/oai#",	
//			"https://arquivojosemariobranco.fcsh.unl.pt/cancoes/oai#",	
//			"https://dicionario-biografico.caravelas.fcsh.unl.pt/dicionario/oai#",	
//			"https://databasemusic-ofm.fcsh.unl.pt/nomes/oai#",	
//			"https://arquivojosemariobranco.fcsh.unl.pt/albuns/oai#",	

//			"https://projetos.dhlab.fcsh.unl.pt/oai#memorias-de-servidao",	
			
//			"https://impactum-journals.uc.pt/mj/oai#mj",
//			"https://www.fcsh.unl.pt/rcl/index.php/rcl/oai#rcl",
//			"https://medievalista.iem.fcsh.unl.pt/index.php/medievalista/oai#medievalista",
//			"https://revistas.rcaap.pt/aham/oai#aham",
//			"http://ojs.letras.up.pt/index.php/tm/oai#tm",
//			"http://oai.openedition.org/#journals:cultura",
//			"http://oai.openedition.org/#journals:sociologico",
//			"http://oai.openedition.org/#journals:etnografica",
		}
		) {
	    	RepositoryWithSolr repository=new RepositoryWithSolr("http://datarossio.dglab.gov.pt:8983/solr/testes-repositorio");
//	    	RepositoryWithSolr repository=new RepositoryWithSolr("http://192.168.111.170:8983/solr/repositorio");
	    	DatasetExporterInHtml exporter=new DatasetExporterInHtml(repository);
	    	File outFile=new File(exportFolder, URLEncoder.encode(sourceId,StandardCharsets.UTF_8.toString())+".html");
//	    	exporter.exportDataset(outFile, sourceId, 100, true);
	    	exporter.exportDataset(outFile, sourceId, -1, true, false);
//	    	exporter.exportDataset(outFile, sourceId, -1, false, false);
		}
	}

	private static List<String> getAllSourceIds() throws IOException {
		List<String> list=new ArrayList<String>();
    	MetadataSources oaiSources=new MetadataSources(new File("src/data/oai_sources.txt"));
    	for(MetadataSource source:oaiSources.getAllSources()) {
    		list.add(source.getSourceId());
    	}
    	return list;
	}
	
}
