package rossio.http;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpMessage;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.ContentType;

import rossio.http.UrlRequest.HttpMethod;
import rossio.util.Global;

public class HttpRequest {
	UrlRequest url;
//	String contentTypeToRequest;
//	CrawlingSession session;
//	Semaphore fetchingSemaphore=new Semaphore(1);
	HttpResponse response;
	Content content;
	Throwable error;

	public HttpRequest(String url) {
		super();
		this.url = new UrlRequest(url);
	}
	
	public HttpRequest(UrlRequest url) {
		super();
		this.url = url;
	}

//	public HttpRequest(UrlRequest url, CrawlingSession session) {
//		super();
//		this.url = url;
//		this.session = session;
//	}



//	public HttpRequest(UrlRequest url, CrawlingSession crawlingSession, Throwable error) {
//		this(url, crawlingSession);
//		this.error=error;
//	}

	public String getUrl() {
		return url.getUrl();
	}
	public UrlRequest getUrlRequest() {
		return url;
	}

	public HttpMethod getHttpMethod() {
		return url.getMethod();
	}
	
//	public void setUrl(UrlRequest url) {
//		this.url = url;
//	}
//
//	public CrawlingSession getSession() {
//		return session;
//	}
//
//	public void setSession(CrawlingSession session) {
//		this.session = session;
//	}
	
	
	/**
	 * May be used by another thread to wait for the request to be processed
	 * @throws IOException 
	 * @throws UnsupportedOperationException 
	 * 
	 * @throws InterruptedException
	 */
//	public void waitForFetchReady() throws InterruptedException {
//		fetchingSemaphore.acquire();
//		fetchingSemaphore.release();
//	}
	
//	public void fetchReady() {
//		fetchingSemaphore.release();
//	}

	public HttpResponse getResponse() {
		return response;
	}

	public void setResponse(HttpResponse response) throws UnsupportedOperationException, IOException {
		this.response = response;
//		if(response.getEntity()!=null) {
//			byte[] byteArray = IOUtils.toByteArray(response.getEntity().getContent());
//			ContentType contentType = ContentType.get(response.getEntity());
//			this.content=new Content(byteArray, contentType);
//		}
//		response.close();
	}
		
//		fetchingSemaphore.release();

	public int getResponseStatusCode() {
		return response.getStatus();
	}
	public byte[] getResponseContent() throws IOException {
		return response.getBody();
	}
	public String getResponseContentAsString() throws IOException {
		ContentType cType=response.getContentTypeParsed();
		if(cType==null || cType.getCharset()==null)
			return new String(getResponseContent(), "UTF8");
		return new String(getResponseContent(), cType.getCharset());
	}
	public String getMimeType() {
		ContentType contentTypeParsed = response.getContentTypeParsed();
		return contentTypeParsed ==null ? null : contentTypeParsed.getMimeType();
	}
	public Charset getCharset() {
		ContentType contentTypeParsed = response.getContentTypeParsed();
		return contentTypeParsed ==null ? null : contentTypeParsed.getCharset();
	}
//
//	public void setContentTypeToRequest(String contentTypeToRequest) {
//		this.contentTypeToRequest = contentTypeToRequest;
//	}

	public void addHeadersToRequest(HttpMessage request) {
		for(SimpleImmutableEntry<String, String> header : url.getHeaders()) {
			request.addHeader(header.getKey(), header.getValue());
		}
	}

//	public List<Header> getResponseHeaders(String... headerNames) {
//		List<Header> meta=new ArrayList<>(5);
//		if(headerNames==null || headerNames.length==0) {
//			for(Header h : getResponse().getAllHeaders())
//				meta.add(h);			
//		}else {
//			for(String headerName: headerNames) {
//	//			for(String headerName: new String[] { "Content-Type", "Content-Encoding", "Content-Disposition", "Link"/*, "Content-MD5"*/}) {
//				for(Header h : getResponse().getHeaders(headerName))
//					meta.add(h);
//			}
//		}
//		return meta;
//	}	
	public String getResponseHeader(String headerName) {
//		List<Header> meta=new ArrayList<>(5);
//		for(Header h : getResponse().getHeaders(headerName))
//			return h.getValue();
//		return null;
		List<Header> meta=new ArrayList<>(5);
		for(Entry<String, String> h : getResponse().getHeaders())
			return h.getKey()+" "+ h.getValue();
//		return null;
		return response.getHeader(headerName);
	}

	public void redirectUrl(String location) {
		this.url=new UrlRequest(location);
		response=null;
		content=null;
		error=null;
	}

	public void fetch() throws InterruptedException, IOException {
		Global.getHttpRequestService().fetch(this);
	}

	public HttpEntity getRequestContent() {
		return url.getRequestContent();
	}

	public int getRetryAttempst() {
		return url.getRetryAttemps();
	}

}
