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

public class run_DatasetExport {
	
	
	public static void main(String[] args) throws Exception {
		String sourceId="https://impactum-journals.uc.pt/mj/oai#mj";
//		String sourceId="https://www.fcsh.unl.pt/rcl/index.php/rcl/oai#rcl";
//		String sourceId="https://medievalista.iem.fcsh.unl.pt/index.php/medievalista/oai#medievalista";
		
    	RepositoryWithSolr repository=new RepositoryWithSolr("http://192.168.111.115:8983/solr/repositorio");
    	DatasetExporter exporter=new DatasetExporter(repository);
    	File outFile=new File("target/"+URLEncoder.encode(sourceId,StandardCharsets.UTF_8)+".ttl.tgz");
    	exporter.exportDataset(outFile, sourceId, -1);
	}
	
}
