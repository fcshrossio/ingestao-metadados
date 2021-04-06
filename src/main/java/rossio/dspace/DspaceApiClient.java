package rossio.dspace;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.fluent.Content;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.DCTerms;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import rossio.data.models.Dc;
import rossio.data.models.DcTerms;
import rossio.data.models.Owl;
import rossio.http.HttpRequest;
import rossio.http.UrlRequest;
import rossio.http.UrlRequest.HttpMethod;
import rossio.util.AccessException;
import rossio.util.Global;
import rossio.util.HttpUtil;
import rossio.util.RdfUtil;
import rossio.util.RdfUtil.Jena;

public class DspaceApiClient {
	
	public interface ModelHandler {
		public boolean handle(Model model);
	}

	String baseUrl;
	String authEntryPoint;
	String itemsEntryPoint;
	String itemsSearchEntryPoint;
	String collectionsEntryPoint;
	
	String authorization = null;
	ObjectMapper jsonMapper = new ObjectMapper();

	String user;
	String password;
	String uriRossioPrefix="http://dados.rossio.fcsh.unl.pt/item/";
	String uriRossioDatasetPrefix="http://dados.rossio.fcsh.unl.pt/conjunto-de-dados/";
	String uriRossioHandlePrefix="http://repositorio.rossio.fcsh.unl.pt/item/";
	
	public DspaceApiClient(String baseUrl, String user, String password) {
		super();
		this.baseUrl = baseUrl;
		this.user = user;
		this.password = password;
		authEntryPoint = baseUrl + "authn/login";
		itemsEntryPoint = baseUrl + "core/items";
		itemsSearchEntryPoint = baseUrl + "discover/search/objects";
		collectionsEntryPoint = baseUrl + "core/collections";
	}

//	http://localhost:8080/server/api/core/items/e8b10c83-00ea-44a3-989e-b2ea08c93aa2

	public void authenticate() throws IOException, AccessException {
		// This code does work. HTTP client cannot parse the http headers in the
		// response
//		throws InterruptedException, IOException {
//		UrlRequest req=new UrlRequest(authEntryPoint+"?user=nunofreire@fcsh.unl.pt&password=aflkaflk", HttpMethod.POST);
//		HttpRequest fetch = Global.getHttpRequestService().fetch(new HttpRequest(req));
//		System.out.println(fetch.getResponseStatusCode());
//		System.out.println( fetch.getResponseContentAsString() );
//		String responseHeader = fetch.getResponseHeader("Authorization");
//		System.out.println(responseHeader);

		String urlParameters = "user=" + user + "&password=" + password;
		URL url = new URL(authEntryPoint);
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setDoOutput(true);
		conn.setRequestMethod("POST");
		conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
		conn.setRequestProperty("charset", "utf-8");
		try (DataOutputStream wr = new DataOutputStream(conn.getOutputStream())) {
			wr.write(urlParameters.getBytes(StandardCharsets.UTF_8));
		}
		
		if(conn.getResponseCode()!=200)
			throw new AccessException(authEntryPoint, conn.getResponseCode());
		
		authorization = conn.getHeaderField("Authorization");
	}

	public String getItem(String uuid) throws IOException, AccessException, InterruptedException {
		if (authorization == null)
			authenticate();
		String jsonResp;
		String urlToRequest=itemsEntryPoint + "/" + uuid;
		try {
			jsonResp = HttpUtil.makeRequestForContent(urlToRequest, "Authorization",
					authorization);
		} catch (IllegalArgumentException e) {
			throw new AccessException(urlToRequest, e);
		}
		return jsonResp;
	}
	public Model getItemMetadata(String uuid) throws IOException, AccessException, InterruptedException {
		String json=getItem(uuid);
//		System.out.println(json);
		JsonNode topNode = jsonMapper.readTree(json);
		JsonNode mdNode = topNode.get("metadata");
		String itemUri=uriRossioPrefix+uuid;
		return parseItemMetadata(itemUri, mdNode);
	}
	private Model parseItemMetadata(String itemUri, JsonNode parentNode) throws IOException, AccessException, InterruptedException {
		Model mdModel=Jena.createModel();
//		mdModel.setNsPrefix("dct", DcTerms.NS);
		Resource itemRes=mdModel.createResource(itemUri);
		Iterator<Entry<String, JsonNode>> fields = parentNode.fields();
		while(fields.hasNext()) {
			Entry<String, JsonNode> fld=fields.next();
			Property fldPred=getPropertyForField(fld.getKey());
			if(fldPred==null)
				continue;
			JsonNode valueArr = fld.getValue();
			for(int i=0; i<valueArr.size(); i++) {
				JsonNode value = valueArr.get(i);
				String v = value.get("value").asText();
				if(v == null ) continue;
				String lg = value.get("language").isNull() ? null : value.get("language").asText();
				if(!StringUtils.isEmpty(v)) {
					if(fld.getKey().equals("dc.identifier.uri"))
						mdModel.add(mdModel.createStatement(itemRes, Owl.sameAs, mdModel.createResource(uriRossioHandlePrefix+v)));
					else if(fldPred.equals(DCTerms.date) ||fldPred.equals(DCTerms.dateSubmitted) || fldPred.equals(DCTerms.available) ) {
						try {
							Calendar parsed = DatatypeConverter.parseDateTime(v);
							Literal valLit = mdModel.createTypedLiteral(parsed);
							mdModel.add(mdModel.createStatement(itemRes, fldPred, valLit));
						} catch (IllegalArgumentException e) {
							Literal valLit = lg==null ? mdModel.createLiteral(v) : mdModel.createLiteral(v, lg);
							mdModel.add(mdModel.createStatement(itemRes, fldPred, valLit));
						}
					} else {
						Literal valLit = lg==null ? mdModel.createLiteral(v) : mdModel.createLiteral(v, lg);
						mdModel.add(mdModel.createStatement(itemRes, fldPred, valLit));
					}
				}
			}	
		}
		return mdModel;
	}
	

	public void getAllItemsMetadataInCollection(String uuidCollection, ModelHandler handler) throws IOException, AccessException, InterruptedException {
		if (authorization == null)
			authenticate();
		String jsonResp;
		String urlToRequest=itemsSearchEntryPoint+"?dsoType=item&scope="+uuidCollection+"&query=*";
		try {
			while (urlToRequest!=null) {
				UrlRequest r=new UrlRequest(urlToRequest);
				jsonResp = HttpUtil.makeRequestForContent(urlToRequest, "Authorization",
						authorization);
	
				JsonNode topNode = jsonMapper.readTree(jsonResp);
				JsonNode searchResultNode = topNode.get("_embedded").get("searchResult");
				JsonNode containerNode = searchResultNode.get("_embedded").get("objects");
				for(int i=0; i<containerNode.size(); i++) {
					JsonNode objNode = containerNode.get(i);
					JsonNode itemNode = objNode.get("_embedded").get("indexableObject");
					JsonNode mdNode = itemNode.get("metadata");
					
					String itemUri=uriRossioPrefix+itemNode.get("uuid").asText();
					Model mdModel = parseItemMetadata(itemUri, mdNode);
					if(!handler.handle(mdModel))
						return;
				}
				
				JsonNode linksNode = searchResultNode.get("_links");
				if(linksNode.get("next")!=null) {
					urlToRequest=linksNode.get("next").get("href").asText();					
				}else
					urlToRequest=null;
			}
		} catch (IllegalArgumentException e) {
			throw new AccessException(urlToRequest, e);
		}
	}

	public String listCollections() throws IOException, AccessException, InterruptedException {
		if (authorization == null)
			authenticate();
		String jsonResp;
		String urlToRequest=collectionsEntryPoint;
		try {
			jsonResp = HttpUtil.makeRequestForContent(urlToRequest, "Authorization",
					authorization);
		} catch (IllegalArgumentException e) {
			throw new AccessException(urlToRequest, e);
		}
		return jsonResp;
	}
	public List<Resource> listCollectionsMetadata() throws IOException, AccessException, InterruptedException {
		String json=listCollections();
//		System.out.println(json);
		List<Resource> collsMd=new ArrayList<Resource>();

		Model mdModel=Jena.createModel();
//		mdModel.setNsPrefix("dct", DcTerms.NS);
		
		JsonNode topNode = jsonMapper.readTree(json);
		JsonNode colsNode = topNode.get("_embedded").get("collections");
		Iterator<JsonNode> colsIt = colsNode.elements();
		while(colsIt.hasNext()) {
			JsonNode colNode = colsIt.next();
			String uuid = colNode.get("uuid").asText();
			Resource datasetRes=mdModel.createResource(uriRossioDatasetPrefix+uuid);
			mdModel.add(mdModel.createStatement(datasetRes, DcTerms.title, colNode.get("name").asText()));
			mdModel.add(mdModel.createStatement(datasetRes, Owl.sameAs, mdModel.createResource(uriRossioHandlePrefix+colNode.get("handle").asText())));
			collsMd.add(datasetRes);
			
			Iterator<Entry<String, JsonNode>> fields = colNode.get("metadata").fields();
			while(fields.hasNext()) {
				Entry<String, JsonNode> fld=fields.next();
				Property fldPred=getPropertyForField(fld.getKey());
				if(fldPred==null)
					continue;
				JsonNode valueArr = fld.getValue();
				for(int i=0; i<valueArr.size(); i++) {
					JsonNode value = valueArr.get(i);
					if(value.get("value").isNull()) continue;
					String v = value.get("value").asText();
					String lg = value.get("language").isNull() ? null : value.get("language").asText();
					if(!StringUtils.isEmpty(v)) {
						if(fldPred.equals(DCTerms.date) ||fldPred.equals(DCTerms.dateSubmitted) || fldPred.equals(DCTerms.available) ) {
							try {
								Calendar parsed = DatatypeConverter.parseDateTime(v);
								Literal valLit = mdModel.createTypedLiteral(parsed);
								mdModel.add(mdModel.createStatement(datasetRes, fldPred, valLit));
							} catch (IllegalArgumentException e) {
								Literal valLit = lg==null ? mdModel.createLiteral(v) : mdModel.createLiteral(v, lg);
								mdModel.add(mdModel.createStatement(datasetRes, fldPred, valLit));
							}
						} else {
							Literal valLit = lg==null ? mdModel.createLiteral(v) : mdModel.createLiteral(v, lg);
							mdModel.add(mdModel.createStatement(datasetRes, fldPred, valLit));
						}
					}
				}	
			}
		}
//		RdfUtil.printOutRdf(mdModel);
		return collsMd;
	}
	
	
	
	private Property getPropertyForField(String key) {
		if(key.equals("dc.date.accessioned")) {
			return DcTerms.dateSubmitted;			
		} else if(key.equals("dc.identifier.uri")) {
			return DcTerms.identifier;
		} else if(key.startsWith("dc.")) {
			key=key.substring(3);
			int indexOfDot = key.indexOf('.');
			if(indexOfDot!=-1) 
				key=key.substring(indexOfDot+1);				
			return Jena.createProperty(DcTerms.NS+key);
		}
		return null;
	}

	public static void main(String[] args) throws Exception {
		Global.init_componentHttpRequestService();

		DspaceApiClient cli = new DspaceApiClient("http://localhost:8080/server/api/", "nunofreire@fcsh.unl.pt",
				"1-3-5-7-9");

		try {
			Model mdModel=cli.getItemMetadata("e8b10c83-00ea-44a3-989e-b2ea08c93aa2---");
			RdfUtil.printOutRdf(mdModel);
			
		} catch (AccessException e) {
			if (e.getCause()==null && e.getCode()!=null) {
				//return same status code
			} else {
				throw e;
			}
		} catch (Exception e) {
			throw e;
		}
	}

}
