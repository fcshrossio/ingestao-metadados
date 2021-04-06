package rossio.ingest.solr;

import java.io.IOException;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CursorMarkParams;

public class SearchIndexWithSolr {
	public interface HitHandler {
		public boolean handle(SolrDocument document) throws Exception;
	}
	
	SolrClient solr;

	public SearchIndexWithSolr(String solrUrl) {
//		final String solrUrl = "http://localhost:8983/solr";
		solr= new HttpSolrClient.Builder(solrUrl)
		    .withConnectionTimeout(10000)
		    .withSocketTimeout(60000)
		    .build();
	}

	public void search(String q, HitHandler handler) throws SolrServerException, IOException {
		final SolrQuery solrQuery = new SolrQuery(q);
		int start=0;
		int rows=20;
		
		boolean done = false;
		QUERY: while (!done) {
		    solrQuery.setStart(start);
		    solrQuery.setRows(rows);
		    QueryResponse rsp = solr.query(solrQuery);
		    SolrDocumentList results = rsp.getResults();
		    
		    System.out.println(results.getNumFound());
		    
			for (SolrDocument document : results) {
		    	try {
					if(!handler.handle(document))
						break QUERY;
				} catch (Exception e) {
					System.err.println("Error handling record: "+document.getFirstValue("id"));
					e.printStackTrace();
					System.err.println("...continuing to next record");
				}
		    }
		    if(results.getNumFound()==0)
		    	done = true;
	        else
	        	start+=rows;
		}
		
	}
	
	
}
