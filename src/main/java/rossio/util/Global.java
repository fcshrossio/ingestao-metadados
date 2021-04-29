package rossio.util;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.regex.Pattern;

import rossio.http.HttpRequestService;

public class Global {
	public static boolean DEBUG = false;

	public static Pattern urlPattern=Pattern.compile("^(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]");
	
	private static HttpRequestService httpRequestService=new HttpRequestService();

	public static HttpRequestService getHttpRequestService() {
		return httpRequestService;
	}
	
	public static void init_componentHttpRequestService() {
		httpRequestService.init();
	}
}
