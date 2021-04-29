package rossio.ingest.solr;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import org.apache.jena.query.Query;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient.RemoteSolrException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CursorMarkParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.StatsParams;
import org.apache.solr.common.util.Base64;
import org.w3c.dom.Element;

public class RepositoryWithSolr {
	public interface ItemHandler {
		public boolean handle(String uuid, byte[] content) throws Exception;
	}
	
	
	SolrClient solr;

	public RepositoryWithSolr(String solrUrl) {
//		final String solrUrl = "http://localhost:8983/solr";
		solr= new HttpSolrClient.Builder(solrUrl)
		    .withConnectionTimeout(10000)
		    .withSocketTimeout(60000)
		    .build();
	}
	
	public void restart() throws SolrServerException, IOException {
	}

	public void abort() throws SolrServerException, IOException {
		solr.rollback();
	}

	public void end() throws SolrServerException, IOException {
		solr.commit();
	}

	public void addItem(String uuid, String source, String identifier, byte[] content) throws SolrServerException, IOException {
		final SolrInputDocument doc = new SolrInputDocument();
		doc.addField("id", uuid);
		doc.addField("rossio_source", source);
		doc.addField("rossio_idAtSource", identifier);	
		doc.addField("rossio_last_update", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(new Date()));	
		doc.addField("rossio_content", content);
		solr.add(doc);
	}
	
	public void commit() throws SolrServerException, IOException {
		solr.commit();
	}

	public void removeAllFrom(String source) throws SolrServerException, IOException {
		try {
			solr.deleteByQuery("rossio_source:"+ClientUtils.escapeQueryChars(source));
			commit();
		} catch (RemoteSolrException e) {
			if(!e.getMessage().contains("undefined field rossio_source"))
				throw e;
		}
	}

	public long getSizeOfSource(String source) throws SolrServerException, IOException {
		final SolrQuery solrQuery = new SolrQuery("*:*");
		solrQuery.addSort("id", ORDER.asc); 
		solrQuery.addField("id");
		solrQuery.addFilterQuery("rossio_source:"+ClientUtils.escapeQueryChars(source));
	    solrQuery.setRows(1);
	    QueryResponse rsp = solr.query(solrQuery);
	    
	    return rsp.getResults().getNumFound();
	}
	
	public void getItemsInSource(String source, ItemHandler handler) throws SolrServerException, IOException {
//		final SolrQuery solrQuery = new SolrQuery("rossio_source:"+ClientUtils.escapeQueryChars(source));
		final SolrQuery solrQuery = new SolrQuery("*:*");
		solrQuery.addSort("id", ORDER.asc); 
		solrQuery.addField("id");
		solrQuery.addField("rossio_content");
		
//		System.out.println(source);

//		solrQuery.addFilterQuery("rossio_source:"+ClientUtils.escapeQueryChars(URLEncoder.encode(source, "UTF8")));
//		System.out.println(URLEncoder.encode(source, "UTF8"));
//		solrQuery.addFilterQuery("rossio_source:"+URLEncoder.encode(source, "UTF8"));
//		System.out.println(ClientUtils.escapeQueryChars(source));
		solrQuery.addFilterQuery("rossio_source:"+ClientUtils.escapeQueryChars(source));
		
		String cursorMark = CursorMarkParams.CURSOR_MARK_START;
		boolean done = false;
		QUERY: while (!done) {
		    solrQuery.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);
		    solrQuery.setRows(100);
		    QueryResponse rsp = solr.query(solrQuery);
		    String nextCursorMark = rsp.getNextCursorMark();
		    for (SolrDocument document : rsp.getResults()) {
		    	try {
					if(!handler.handle(document.getFirstValue("id").toString(), (byte[])document.getFirstValue("rossio_content")))
						break QUERY;
				} catch (Exception e) {
					System.err.println("Error handling record: "+document.getFirstValue("id"));
					e.printStackTrace();
					System.err.println("...continuing to next record");
				}
		    }
		    if (cursorMark.equals(nextCursorMark)) {
		        done = true;
		    }
		    cursorMark = nextCursorMark;
		}
		
//		
//		
//		
//
//		int start=0;
//		int results=-1;
//		QUERY: while (results==-1 || start<results) {
//			final SolrQuery query = new SolrQuery("rossio_source:"+source);
//			query.setRows(100);
//			query.setStart(start+1);
//			final QueryResponse response = solr.query(query);
//			final SolrDocumentList documents = response.getResults();
//			results=response.get
//			for(SolrDocument document : documents) {
//				
//			}
//		}
		
	}

	public byte[] getItem(String uuid) throws SolrServerException, IOException {
		SolrDocument it=solr.getById(uuid);
		return it==null? null : (byte[]) it.getFirstValue("rossio_content");		
	}

	public String getRecordUuid(String sourceId, String identifier) throws SolrServerException, IOException {
		SolrQuery solrQuery = new SolrQuery();
        solrQuery.setQuery("rossio_idAtSource:"+ClientUtils.escapeQueryChars(identifier)+
        		" AND rossio_source:"+ClientUtils.escapeQueryChars(sourceId) );
		solrQuery.addField("id");
        solrQuery.setStart(0);
        solrQuery.setRows(1);

        QueryResponse queryResponse = solr.query(solrQuery);
        SolrDocumentList solrDocs = queryResponse.getResults();
        Iterator<SolrDocument> iterator = solrDocs.iterator();
        if (!iterator.hasNext()) 
        	return null;
        SolrDocument solrDocument = iterator.next();
        String docId = (String) solrDocument.getFieldValue("id");
		return docId;
	}

	public void delete(String uuid) throws SolrServerException, IOException {
		solr.deleteById(uuid);
	}
}
