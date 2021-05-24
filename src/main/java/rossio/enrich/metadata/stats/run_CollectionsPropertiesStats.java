package rossio.enrich.metadata.stats;

import java.io.File;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Map;

import org.apache.commons.io.output.FileWriterWithEncoding;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.Property;

import rossio.data.models.DcTerms;
import rossio.enrich.metadata.stats.CreateStatsTask.PropertyStats;
import rossio.ingest.solr.RepositoryWithSolr;
import rossio.ingest.solr.manager.OaiSource;
import rossio.ingest.solr.manager.OaiSources;

public class run_CollectionsPropertiesStats {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
//    	RepositoryWithSolr repository=new RepositoryWithSolr("http://192.168.111.115:8983/solr/repositorio");
//    	RepositoryWithSolr repository=new RepositoryWithSolr("http://datarossio.dglab.gov.pt:8983/solr/repositorio");
    	RepositoryWithSolr repository=new RepositoryWithSolr("http://192.168.111.170:8983/solr/repositorio");
		CreateStatsTask enrichTast=new CreateStatsTask();
		enrichTast.testing=true;
		
		
//		OaiSources oaiSources=new OaiSources(new File("src/data/oai_sources-test1.txt"));
		OaiSources oaiSources=new OaiSources(new File("src/data/oai_sources-test-long.ttl"));
    	for(OaiSource source:oaiSources.getAllSources()) {
    		Map<Property, PropertyStats> stats = enrichTast.runOnCollectionMultiProp(repository, 
    				source.getSourceId(), DcTerms.subject, DcTerms.contributor, DcTerms.creator);
    		
    		File exportFolder=new File("C:\\Users\\nfrei\\Desktop\\ROSSIO_exports");    		

    		for (Property p: stats.keySet()) {
//    			String filename=URLEncoder.encode(source.getSourceId(), StandardCharsets.UTF_8);
    			String filename=URLEncoder.encode(source.name, StandardCharsets.UTF_8);
    			if(StringUtils.isEmpty(filename))
    				filename=URLEncoder.encode(source.getSourceId(), StandardCharsets.UTF_8);
    			
    			FileWriterWithEncoding writer=new FileWriterWithEncoding(new File("C:\\Users\\nfrei\\Desktop\\ROSSIO_exports\\"+p.getLocalName()+"\\"+filename+".html"), StandardCharsets.UTF_8);
    			stats.get(p).toStringHtml(writer, ""+(StringUtils.isEmpty(source.name) ? source.getSourceId() : source.name) +"<br />Propriedade:"+p.getURI());
    			writer.close();    			
    		}
    		
    	}
		
		
	}

}
