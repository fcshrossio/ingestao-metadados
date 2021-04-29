package rossio.enrich.linkprovider;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;

import rossio.data.models.DcTerms;
import rossio.http.HttpRequest;
import rossio.util.AccessException;
import rossio.util.HttpUtil;
import rossio.util.RdfUtil;

public class ChoLinksUtil {
	
	public static LinkTest testLink(String url) {
		LinkTest test=new LinkTest(url);	
//		System.out.println(url);
		try {
//			HttpRequest headReq = HttpUtil.makeHeadRequest(value);		
			HttpRequest headReq = HttpUtil.makeRequest(url);	
			if(headReq.getResponse().getStatus()==200) {
				String mime=headReq.getResponse().getContentType();
				if(mime!=null) {
					String mimeLc=mime.toLowerCase();
					test.setResult(mime);
				}else
					test.setResult("unknown");
			} else
				test.setResult("status code: "+headReq.getResponse().getStatus());
		} catch (IllegalArgumentException e) {
			test.setResult("ERROR-URL_SYNTAX");
		} catch (AccessException | InterruptedException e) {
			test.setResult("ERROR-ACCESS");
		}
		return test;
	}
	
	public static ArrayList<LinkTest> testAllLinks(Resource choRes, Property... inProperties) {
		ArrayList<LinkTest> tests=new ArrayList<LinkTest>();
		ArrayList<String> identifiers=new ArrayList<String>();
		for(Property p: inProperties)
			identifiers.addAll(listUrls(choRes, p));
		for(String url: identifiers) 
			tests.add( testLink(url));
		return tests;
	}
	

	protected static ArrayList<String> listUrls(Resource choRes, Property identifier) {
		ArrayList<String> identifiers=new ArrayList<String>();
		List<Statement> propsList = choRes.listProperties(DcTerms.identifier).toList();
		propsList.addAll(choRes.listProperties(DcTerms.relation).toList());
		for(Statement st: propsList) {
			if(st.getObject().isLiteral()) 
				identifiers.add(st.getObject().asLiteral().getString());
			else if(st.getObject().isURIResource())
				identifiers.add(st.getObject().asResource().getURI());
			else if(RdfUtil.isSeq(st.getObject().asResource())) {
				for(RDFNode node: RdfUtil.getAsSeq(st.getObject().asResource()).iterator().toList()) {
					if(node.isLiteral()) 
						identifiers.add(node.asLiteral().getString());
					else if(node.isURIResource())
						identifiers.add(node.asResource().getURI());
				}
			}

		}
		
		for(Iterator<String> it=identifiers.iterator(); it.hasNext() ;) {
			String url=it.next();
			if(url==null) 
				it.remove();
			else {
				String valueLc=url.toLowerCase();
				if(!valueLc.startsWith("http://") && !valueLc.startsWith("https://")) 
					it.remove();
			}
		}
		return identifiers;
	}
}
