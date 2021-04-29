package rossio.enrich.linkprovider;

public class LinkTest {
	String url;
	String result;
	
	public LinkTest(String url) {
		super();
		this.url = url;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getResult() {
		return result;
	}
	public void setResult(String result) {
		this.result = result;
	}
	public String getUrlForPrint() {
		return url.replaceAll("&", "&amp;");
	}
}
