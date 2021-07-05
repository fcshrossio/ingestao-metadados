package rossio.script;

import java.io.File;
import java.io.FileOutputStream;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.oclc.oai.harvester2.verb.ListSets;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import rossio.data.models.Dcat;
import rossio.data.models.Rdf;
import rossio.data.models.Rossio;
import rossio.ingest.solr.manager.OaiSource;
import rossio.ingest.solr.manager.OaiSources;
import rossio.util.RdfUtil;
import rossio.util.RdfUtil.Jena;
import rossio.util.XmlUtil;

public class ConvertTxtConfigToRdf {

	public static void main(String[] args) throws Exception {
		String filename="C:\\Users\\nfrei\\Desktop\\oai_sources.txt";
		String outFilename="C:\\Users\\nfrei\\Desktop\\oai_sources.ttl";
//		String filename="src\\data\\oai_sources-test-very_long.txt";
//		String outFilename="src\\data\\oai_sources-test-very_long.ttl";
//		String filename="oai_sources-test-long.txt";
//		String outFilename="oai_sources-test-long.ttl";
		
		OaiSources oaiSources=OaiSources.readFromTxt(new File(filename));
		
		oaiSources.setSourcesFile(new File(outFilename));
		
		for(OaiSource s: oaiSources.getAllSources()) {
			if(s.set!=null && StringUtils.isEmpty(s.name)) {
				try {
					s.updateName();
				} catch (Exception e) {
					e.printStackTrace();
					System.out.println(s.getSourceIdDeprecated()+ " - Ignore and proceed");
				}
			}
		}
		
		oaiSources.save();
		

	}
	
	
}
