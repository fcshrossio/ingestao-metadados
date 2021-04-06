package rossio.wordpress;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.jsoup.Jsoup;

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

import rossio.wordpress.WordpressClient.PageHandler;
import rossio.wordpress.WordpressClient.PostHandler;

public class TestWordpress {
	
	public static void main(String[] args) throws Exception {
		String baseUrl = "https://clunl.fcsh.unl.pt";
//		String baseUrl = "https://www.fcsh.unl.pt/devp";
		
		
		StringBuffer sb=new StringBuffer();
		sb.append("<html><body>");
		
		WordpressClient wpCli=new WordpressClient(baseUrl);
		int[] cnt=new int[] {0};
//		wpCli.getAllPages(new PageHandler() {
//			public boolean handle(Page post) {
//				System.out.println(Jsoup.parse( post.getTitle().getRendered() ).wholeText());
//				System.out.println(Jsoup.parse( post.getContent().getRendered() ).wholeText());
//				System.out.println(Jsoup.parse( post.getExcerpt().getRendered() ).wholeText());
//				
//				System.out.println(post.getAdditionalProperties());
//				cnt[0]++;
//				return true;
//			}
//		});
		wpCli.getAllPosts(new PostHandler() {
			public boolean handle(Post post) throws Exception {
				sb.append("<br /><table border=\"1\">");
				sb.append("<tr><td>Title</td><td>"+Jsoup.parse( post.getTitle().getRendered() ).wholeText()+"</td></tr>");
				sb.append("<tr><td>Content</td><td>"+Jsoup.parse( post.getContent().getRendered() ).wholeText()+"</td></tr>");
				sb.append("<tr><td>Date</td><td>"+post.getDate()+"</td></tr>");
				sb.append("<tr><td>Format</td><td>"+post.getFormat()+"</td></tr>");
				sb.append("<tr><td>Link</td><td>"+post.getLink()+"</td></tr>");
				sb.append("<tr><td>Status</td><td>"+post.getStatus()+"</td></tr>");
				sb.append("<tr><td>Type</td><td>"+post.getType()+"</td></tr>");
				
				User user = wpCli.getUser(post.getAuthor());
				sb.append("<tr><td>Author</td><td>"+user.getName()+"</td></tr>");
//				sb.append("<tr><td>Author</td><td>"+user.getFirstName()+" "+user.getLastName()+"</td></tr>");
				
				sb.append("</table>");
				cnt[0]++;
				return true;
			}
		});
		System.out.println(cnt[0]+" resultados:");
		
		System.out.println(sb.toString());
		FileUtils.write(new File("c:/users/nfrei/desktop/clunl-wordpress.html"), sb);
		
	}
	
	
}
