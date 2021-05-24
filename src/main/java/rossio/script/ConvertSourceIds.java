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
import rossio.ingest.solr.RepositoryWithSolr;
import rossio.ingest.solr.RepositoryWithSolr.ItemHandler;
import rossio.ingest.solr.manager.OaiSource;
import rossio.ingest.solr.manager.OaiSources;
import rossio.util.RdfUtil;
import rossio.util.RdfUtil.Jena;
import rossio.util.XmlUtil;

public class ConvertSourceIds {

	public static void main(String[] args) throws Exception {
		String filename="src/data/oai_sources-test-short.ttl";
		String solrRepoUrlSource="http://datarossio.dglab.gov.pt:8983/solr/testes-repositorio";
		String solrRepoUrlTarget="http://datarossio.dglab.gov.pt:8983/solr/testes-repositorio";
		if (args!=null && args.length>=3) {
			filename=args[0];
			solrRepoUrlSource=args[1];
			solrRepoUrlTarget=args[2];		
		}

    	RepositoryWithSolr repositorySource=new RepositoryWithSolr(solrRepoUrlSource);
    	RepositoryWithSolr repositoryTarget=new RepositoryWithSolr(solrRepoUrlTarget);
    	
		OaiSources oaiSources=new OaiSources(new File(filename));
		for(OaiSource s: oaiSources.getAllSources()) {
			System.out.println("convert "+s.getSourceIdDeprecated());
			repositorySource.getItemsInSource(s.getSourceIdDeprecated(), new ItemHandler() {
				int cnt=0;
				@Override
				public boolean handle(String uuid, String idAtSource, String lastUpdate, byte[] content) throws Exception {
					cnt++;
					repositoryTarget.addItem(uuid, s.getSourceId(), idAtSource, content);
					if(cnt%200==0) {
						System.out.println(cnt);
						repositoryTarget.commit();
					}
					return true;
				}
			});
			repositoryTarget.commit();
		}
		

	}
	
	
}
