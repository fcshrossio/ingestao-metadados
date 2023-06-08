package rossio.script;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.HashSet;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.oclc.oai.harvester2.verb.ListSets;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import rossio.data.models.Dc;
import rossio.data.models.DcTerms;
import rossio.data.models.Dcat;
import rossio.data.models.Rdf;
import rossio.data.models.Rossio;
import rossio.ingest.preprocess.DglabPreprocessor;
import rossio.ingest.solr.RepositoryWithSolr;
import rossio.ingest.solr.RepositoryWithSolr.FetchOption;
import rossio.ingest.solr.RepositoryWithSolr.ItemHandler;
import rossio.ingest.solr.manager.MetadataSource;
import rossio.ingest.solr.manager.MetadataSources;
import rossio.util.RdfUtil;
import rossio.util.RdfUtil.Jena;
import rossio.util.XmlUtil;

public class ChangeIngestedDatasetsDglab {

	public static void main(String[] args) throws Exception {
		HashSet<String> dataproviderUris=new HashSet<String>();
//		dataproviderUris.add("http://vocabs.rossio.fcsh.unl.pt/agentes/c_5672cf6f");
		dataproviderUris.add("http://vocabs.rossio.fcsh.unl.pt/agentes/c_4229e047");
		dataproviderUris.add("http://vocabs.rossio.fcsh.unl.pt/agentes/c_f9e05c96");
		dataproviderUris.add("http://vocabs.rossio.fcsh.unl.pt/agentes/c_5062b87f");
		dataproviderUris.add("http://vocabs.rossio.fcsh.unl.pt/agentes/c_50a5323c");
		dataproviderUris.add("http://vocabs.rossio.fcsh.unl.pt/agentes/c_6da5082f");
		dataproviderUris.add("http://vocabs.rossio.fcsh.unl.pt/agentes/c_819ef5cf");
		dataproviderUris.add("http://vocabs.rossio.fcsh.unl.pt/agentes/c_53e2aa91");
		
		String filename="src/data/oai_sources_debug.ttl";
		String solrRepoUrl="http://192.168.111.170:8983/solr/repositorio";
//		String solrRepoUrlOut="http://datarossio.dglab.gov.pt:8983/solr/repositorio";
		String solrRepoUrlOut=null;
		
		final boolean testing;
		if (args!=null && args.length>=2) {
			filename=args[0];
			solrRepoUrl=args[1];
			solrRepoUrlOut=args[2];
			testing=false;
		} else
			testing = true;
//			testing=false;

		RepositoryWithSolr repository=new RepositoryWithSolr(solrRepoUrl);
    	RepositoryWithSolr repositoryOut;
    	if(solrRepoUrlOut==null || solrRepoUrl.equals(solrRepoUrlOut))
    		repositoryOut=repository;
    	else
    		repositoryOut=new RepositoryWithSolr(solrRepoUrl);
    	
		MetadataSources oaiSources=new MetadataSources(new File(filename));
		for(MetadataSource s: oaiSources.getAllSources()) {
			if (dataproviderUris.contains(s.dataProvider)) {
				System.out.println("Processing "+s.getSourceId());
				repository.getItemsInSource(s.getSourceId(), FetchOption.BOTH_VERSIONS, new ItemHandler() {
					int recCnt=0;
					int recSkip=0;
					@Override
					public boolean handle(String uuid, String identifierAtSource, String lastHarvestTimestamp, byte[] contentAtSource, byte[] contentRossio)
							throws Exception {
						RDFParser reader = RDFParser.create().lang(Lang.RDFTHRIFT).source(new ByteArrayInputStream(contentAtSource)).build();
						Model model = Jena.createModel();
						reader.parse(model);
						Resource cho = model.createResource(Rossio.NS_ITEM+uuid);
						
//						RdfUtil.printOutRdf(model);
						
						Statement subject = cho.getProperty(DcTerms.subject);
						if(subject!=null) {

//							RdfUtil.printOutRdf(model);
							DglabPreprocessor.moveSubjectToDescription(cho);
//System.out.println("--->");
//							RdfUtil.printOutRdf(model);
//							System.out.println("EEEEEEEEEEEEEEEEEEEEENNNNNNNNNDDDDDDDDDDD");
							
							
							repositoryOut.updateItem(uuid, s.getSourceId(), identifierAtSource, RdfUtil.serializeToRdfRift(model), contentRossio);
							recCnt++;
//						if(recCnt % 20000==0)
							if(recCnt % 20==0) 
								System.out.println("updated rec "+uuid +" ("+recCnt+")");
							if(recCnt % 100==0) {
								repositoryOut.commit();
								System.out.println("commit"); 
							}
							return testing ? recCnt<20 : true;
						} else {
							recSkip++;
							if(recSkip % 1000==0)
								System.out.println("Skipped "+recSkip);
						}
						return true;
					}
				});
				repositoryOut.commit();
			}
		}
	}
	
	
}
