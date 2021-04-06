package rossio.wordpress;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.CursorMarkParams;

import com.afrozaar.wordpress.wpapi.v2.Wordpress;
import com.afrozaar.wordpress.wpapi.v2.config.ClientConfig;
import com.afrozaar.wordpress.wpapi.v2.config.ClientFactory;
import com.afrozaar.wordpress.wpapi.v2.exception.UserNotFoundException;
import com.afrozaar.wordpress.wpapi.v2.model.Page;
import com.afrozaar.wordpress.wpapi.v2.model.Post;
import com.afrozaar.wordpress.wpapi.v2.model.User;
import com.afrozaar.wordpress.wpapi.v2.request.Request;
import com.afrozaar.wordpress.wpapi.v2.request.SearchRequest;
import com.afrozaar.wordpress.wpapi.v2.response.PagedResponse;

public class WordpressClient {

	public interface PostHandler {
		public boolean handle(Post post) throws Exception;
	}
	public interface PageHandler {
		public boolean handle(Page post)  throws Exception;
	}
	
	final Wordpress client;
//	String baseUrl = "https://www.fcsh.unl.pt/devp";
//	String username = "";
//	String password = "";
	
	public WordpressClient(String baseUrl) {
		super();
		client = ClientFactory.fromConfig(ClientConfig.of(baseUrl, null, null, false, false));
	}

	
	public void getAllPosts(PostHandler handler) {
		try {
	//		boolean done = false;
			Optional<String> next=null;
			QUERY: do {
				final PagedResponse<Post> response;
				
				if(next==null)
					response = client.search(SearchRequest.Builder.aSearchRequest(Post.class).build());
				else			
					response=client.getPagedResponse(new URI(next.get().toString()), Post.class);
				
				for (Post post: response.getList()) {
			    	try {
						if(!handler.handle(post))
							break QUERY;
					} catch (Exception e) {
						System.err.println("Error handling record: "+post.getId());
						e.printStackTrace();
						System.err.println("...continuing to next post");
					}
			    }
	//		    if (response.getNext()==null) {
	//		        done = true;
	//		    }else
			    	next = response.getNext();
			} while (next.isPresent());
		} catch (URISyntaxException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}
	
	public void getAllPages(PageHandler handler) {
		try {
	//		boolean done = false;
			Optional<String> next=null;
			QUERY: do {
				final PagedResponse<Page> response;
				
				if(next==null)
					response = client.search(SearchRequest.Builder.aSearchRequest(Page.class).build());
				else			
					response=client.getPagedResponse(new URI(next.get().toString()), Page.class);
				
				for (Page post: response.getList()) {
			    	try {
						if(!handler.handle(post))
							break QUERY;
					} catch (Exception e) {
						System.err.println("Error handling record: "+post.getId());
						e.printStackTrace();
						System.err.println("...continuing to next post");
					}
			    }
	//		    if (response.getNext()==null) {
	//		        done = true;
	//		    }else
			    	next = response.getNext();
			} while (next.isPresent());
		} catch (URISyntaxException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}


	public User getUser(Long author) throws UserNotFoundException {
		return client.getUser(author);
	}
}
