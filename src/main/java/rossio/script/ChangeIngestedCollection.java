package rossio.script;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.oclc.oai.harvester2.verb.ListSets;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import rossio.data.models.Dcat;
import rossio.data.models.Rdf;
import rossio.data.models.Rossio;
import rossio.ingest.preprocess.DglabPreprocessor;
import rossio.ingest.solr.RepositoryWithSolr;
import rossio.ingest.solr.RepositoryWithSolr.FetchOption;
import rossio.ingest.solr.RepositoryWithSolr.ItemHandler;
import rossio.ingest.solr.manager.OaiSource;
import rossio.ingest.solr.manager.OaiSources;
import rossio.util.RdfUtil;
import rossio.util.RdfUtil.Jena;
import rossio.util.XmlUtil;

public class ChangeIngestedCollection {

	public static void main(String[] args) throws Exception {
		String filename="src/data/oai_sources-test-short.ttl";
		String solrRepoUrl="http://datarossio.dglab.gov.pt:8983/solr/testes-repositorio";
		final boolean testing;
		if (args!=null && args.length>=2) {
			filename=args[0];
			solrRepoUrl=args[1];
			testing=false;
		} else
			testing = true;

    	RepositoryWithSolr repository=new RepositoryWithSolr(solrRepoUrl);
    	
		OaiSources oaiSources=new OaiSources(new File(filename));
		for(OaiSource s: oaiSources.getAllSources()) {
			if (s.preprocessor!=null && s.preprocessor instanceof DglabPreprocessor) {
				repository.getItemsInSource(s.getSourceId(), FetchOption.BOTH_VERSIONS, new ItemHandler() {
					int recCnt=0;
					@Override
					public boolean handle(String uuid, String identifierAtSource, String lastHarvestTimestamp, byte[] contentAtSource, byte[] contentRossio)
							throws Exception {
						RDFParser reader = RDFParser.create().lang(Lang.RDFTHRIFT).source(new ByteArrayInputStream(contentRossio)).build();
						Model model = Jena.createModel();
						reader.parse(model);

						DglabPreprocessor.moveSubjectToDescription(model.createResource(Rossio.NS_ITEM+uuid));
						
						repository.updateItem(uuid, s.getSourceId(), identifierAtSource, contentAtSource, 
								RdfUtil.serializeToRdfRift(model));
						
						recCnt++;
						if(recCnt % 20000==0)
							repository.commit();
						return testing ? recCnt<100 : true;
					}
				});
				repository.commit();
			}
		}
	}
	
	
}
