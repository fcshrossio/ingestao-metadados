package rossio.script;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.oclc.oai.harvester2.verb.ListSets;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.core.JsonParser;

import rossio.data.models.Dcat;
import rossio.data.models.Rdf;
import rossio.data.models.Rossio;
import rossio.data.models.Skos;
import rossio.ingest.solr.RepositoryWithSolr;
import rossio.ingest.solr.RepositoryWithSolr.ItemHandler;
import rossio.ingest.solr.manager.MetadataSource;
import rossio.ingest.solr.manager.MetadataSources;
import rossio.util.Global;
import rossio.util.RdfUtil;
import rossio.util.RdfUtil.Jena;
import rossio.util.XmlUtil;

public class CreateRepositoryReport {

	public static void main(String[] args) throws Exception {
		String filename="src/data/oai_sources.ttl";
		String outFilename="target/oai_sources_report.csv";
//		String solrRepoUrlSource="http://datarossio.dglab.gov.pt:8983/solr/repositorio";
		String solrRepoUrlSource="http://192.168.111.170:8983/solr/repositorio";
		if (args!=null && args.length>=3) {
			filename=args[0];
			solrRepoUrlSource=args[1];
		}

		Global.init_componentHttpRequestService();
		
    	RepositoryWithSolr repo=new RepositoryWithSolr(solrRepoUrlSource);
    	
    	Map<String, Integer> sourcesSizes=repo.getSourcesSizes();

    	StringBuilder out=new StringBuilder();
    	
		MetadataSources oaiSources=new MetadataSources(new File(filename));
		for(MetadataSource s: oaiSources.getAllSources()) {
			Model providerMd = RdfUtil.readRdfFromUri(s.dataProvider);
			Resource dpRes = providerMd.createResource(s.dataProvider);
			String dpLabel = dpRes.getProperty(Skos.prefLabel).getObject().asLiteral().getString();
			out.append("\""+s.getSourceId()+"\",\""+dpLabel+"\",\""+s.name+"\","+
//					out.append("\""+s.name+"\",\""+dpLabel+"\","+
					sourcesSizes.get(s.getSourceId()) ).append("\n");
//			out.append("\""+s.getSourceIdDeprecated()+"\",\""+dpLabel+"\",\""+s.name+"\","+
////					out.append("\""+s.name+"\",\""+dpLabel+"\","+
//sourcesSizes.get(s.getSourceIdDeprecated()) ).append("\n");
		}
		
		FileUtils.write(new File(outFilename), out.toString(), StandardCharsets.UTF_8);
	}
}
