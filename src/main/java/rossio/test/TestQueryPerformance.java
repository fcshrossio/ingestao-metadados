package rossio.test;

import java.util.Date;

import org.apache.solr.common.SolrDocument;

import rossio.ingest.solr.SearchIndexWithSolr;
import rossio.ingest.solr.SearchIndexWithSolr.HitHandler;

public class TestQueryPerformance {

	public static final String[] queries=new String[] {
			"filosofia",
			"casamento",
			"dcterms_title:filosofia",
			"dcterms_title:casamento",
			"dcterms_description:filosofia",
			"dcterms_creator:silva"
	};
	
	
	public static void main(String[] args) throws Exception {
		SearchIndexWithSolr searchIndex=new SearchIndexWithSolr("http://datarossio.dglab.gov.pt:8984/solr/pesquisa");
		
		
		for(String q: queries) {
			Date[] startEnd=new Date[] {
					new Date(), null
			};
			searchIndex.search(q, new HitHandler() {
				public boolean handle(SolrDocument document) throws Exception {
					startEnd[1]=new Date();
					return false;
				}
			});
			
			System.out.println("Query: "+ q);
			System.out.println("Time: "+ (startEnd[1].getTime()-startEnd[0].getTime()));
		}
		
		
	}
	
	
	
}
