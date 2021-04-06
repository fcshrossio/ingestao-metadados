package rossio.util;

import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
//import javax.ws.rs.client.Client;
//import javax.ws.rs.client.ClientBuilder;


/**
 * Utility class to initialise the usage of SSL certificates (self signed by IIIF implementors)
 * 
 * @author Nuno
 *
 */
public class HttpsUtil {
	public static void initSslTrustingHostVerifier() {
	    TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager() {

	        @Override
	        public X509Certificate[] getAcceptedIssuers() {
	            // TODO Auto-generated method stub
	            return null;
	        }

	        @Override
	        public void checkServerTrusted(X509Certificate[] arg0, String arg1)
	                throws CertificateException {
	            // TODO Auto-generated method stub

	        }

	        @Override
	        public void checkClientTrusted(X509Certificate[] arg0, String arg1)
	                throws CertificateException {
	            // TODO Auto-generated method stub

	        }
	    }};

	    // Install the all-trusting trust manager
	    try {
//	        SSLContext sc = SSLContext.getInstance("TLS");
	        SSLContext sc = SSLContext.getInstance("SSL");
	        sc.init(null, trustAllCerts, new SecureRandom());
	        HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
	    } catch (Exception e) {
	        ;
	    }	
		
	}
	
		public static void initSslTrustingHostVerifierOld() {
	        try {
	            // Create a trust manager that does not validate certificate chains
	            TrustManager[] trustAllCerts = new TrustManager[] {new X509TrustManager() {
	                    @Override
	                    public java.security.cert.X509Certificate[] getAcceptedIssuers() {
	                        return null;
	                    }
	                    @Override
	                    public void checkClientTrusted(X509Certificate[] certs, String authType) {
	                    }
	                    @Override
	                    public void checkServerTrusted(X509Certificate[] certs, String authType) {
	                    }
	                }
	            };
	     
	            // Install the all-trusting trust manager
	            SSLContext sc = SSLContext.getInstance("SSL");
//	            SSLContext sc = SSLContext.getInstance("TLS");
	            sc.init(null, trustAllCerts, new java.security.SecureRandom());
	            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
	     
	            // Create all-trusting host name verifier
	            HostnameVerifier allHostsValid = new HostnameVerifier() {

					@Override
					public boolean verify(String hostname, SSLSession session) {
						return true;
					}
	            };
	            // Install the all-trusting host verifier
	            HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
	        } catch (Exception e) {
                e.printStackTrace();
            }

	}

//		public static Client initSslTrustingHostVerifierForHttpClient() throws Exception{
//		       try {
//		            // Create a trust manager that does not validate certificate chains
//		            TrustManager[] trustAllCerts = new TrustManager[] {new X509TrustManager() {
//		                    @Override
//		                    public java.security.cert.X509Certificate[] getAcceptedIssuers() {
//		                        return null;
//		                    }
//		                    @Override
//		                    public void checkClientTrusted(X509Certificate[] certs, String authType) {
//		                    }
//		                    @Override
//		                    public void checkServerTrusted(X509Certificate[] certs, String authType) {
//		                    }
//		                }
//		            };
//		     
//		            // Install the all-trusting trust manager
//		            SSLContext sc = SSLContext.getInstance("SSL");
////		            SSLContext sc = SSLContext.getInstance("TLS");
//		            sc.init(null, trustAllCerts, new java.security.SecureRandom());
//		            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
//		     
//		            // Create all-trusting host name verifier
//		            HostnameVerifier allHostsValid = new HostnameVerifier() {
//
//						@Override
//						public boolean verify(String hostname, SSLSession session) {
//							return true;
//						}
//		            };
//		            // Install the all-trusting host verifier
//		            HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
//
//		       
//		            
//		            ClientBuilder clientBuilder= ClientBuilder.newBuilder()
//		                    
//		                            .hostnameVerifier(allHostsValid)
//		                            .sslContext(sc)
//		                            .register(
//		                            MultiPartFeature.class);
////		                    if(ignoreSslCertificate) {
////		                        clientBuilder=clientBuilder.hostnameVerifier(allHostsValid);
////		                    }
//		                    Client client = clientBuilder.build();
//		                    return client;
//		            
////		            SSLContext sslContext = b2shareApi.getApiClient().getHttpClient().getSslContext();
////		            sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
//		       
////		            b2shareApi.getApiClient().getHttpClient().getHostnameVerifier().;
//		       
//		       } catch (Exception e) {
//	                e.printStackTrace();
//	                throw e;
//	            }
//			
//		}


}
