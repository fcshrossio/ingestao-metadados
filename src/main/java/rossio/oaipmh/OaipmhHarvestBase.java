package rossio.oaipmh;

import java.io.CharConversionException;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.oclc.oai.harvester2.verb.HarvesterVerb;
import org.oclc.oai.harvester2.verb.ListRecords;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

/**
 * Provides an Iterator interface to an OAI-PMH harvest
 * 
 * @author Nuno Freire (nfreire@gmail.com)
 * @since 21 de Fev de 2011
 */
public abstract class OaipmhHarvestBase {
	
	public interface WarnHandler {
		public void recoveredError(String lostResumptionToken, String lostResponse, String recoveredResumptionToken);

		public void unrecoveredError(String lostResumptionToken, String lostResponse);
	}
	
    protected final String              baseURL;
    protected final String              metadataPrefix;
    protected final String              setSpec;
    protected final String              from;
    protected final String              until;

    protected int                       maxRecordsToRetrieve  = -1;

    protected String                    nextResumptionToken   = null;
    protected int                       totalRetrievedRecords = 0;
    
    protected double                    movingAverage         = 0;
    protected long                      movingSteps           = 0;

    protected long                      movingDiff            = 0;
    protected long                      movingCount           = 0;
    
    protected File saveNextResumptionTokenIn;
    protected File saveNextResponseIn;
    
    protected WarnHandler warnHandler;
    
    boolean fetchedFirst=true;
    
    /**
     * The total records as reported by the OAI-PMH server on the first ListRecords response
     */
    protected int                       completeListSize      = -1;

    protected final Queue<OaiPmhRecord> records               = new LinkedList<OaiPmhRecord>();

    protected int retriesOnError=3;
    
    
    /**
     * Creates a new instance of this class.
     * 
     * @param baseURL
     *            Oai server base url
     * @param metadataPrefix
     *            oai metadata prefix to harvest
     * @param setSpec
     *            oai set to harvest
     * @throws HarvestException
     */
    public OaipmhHarvestBase(String baseURL, String metadataPrefix, String setSpec) throws HarvestException {
        this(baseURL, null, null, metadataPrefix, setSpec);
    }

    /**
     * Creates a new instance of this class.
     * 
     * @param baseURL
     *            Oai server base url
     * @param from
     * @param until
     * @param metadataPrefix
     *            oai metadata prefix to harvest
     * @param setSpec
     *            oai set to harvest
     * @throws HarvestException
     */
    public OaipmhHarvestBase(String baseURL, String from, String until, String metadataPrefix,
                         String setSpec) throws HarvestException {
        super();
        this.baseURL = baseURL;
        this.from = from;
        this.until = until;
        this.metadataPrefix = metadataPrefix;
        this.setSpec = setSpec;

        fetchedFirst=false;
    }

    /**
     * Creates a new instance of this class.
     * 
     * @param baseURL
     *            Oai server base url
     * @param resumptionToken
     */
    public OaipmhHarvestBase(String baseURL, String resumptionToken) {
        super();
        this.baseURL = baseURL;
        this.setSpec = null;
        this.metadataPrefix = null;
        this.from = null;
        this.until = null;

        this.nextResumptionToken = resumptionToken;
        fetchedFirst=true;
//        try {
//            fetchNextIfEmpty();
//        } catch (HarvestException e) {
//            throw new RuntimeException("Could not get the first records!", e);
//        }
    }

    protected synchronized void fetchFirstRecords() throws HarvestException {
        try {
            int tries = 0;
            while (true) {
                tries++;
                ListRecords listRecords = null;
                try {
                    long start = System.currentTimeMillis();
                    listRecords = new ListRecords(baseURL, from, until, setSpec, metadataPrefix,saveNextResponseIn);
                    movingDiff = System.currentTimeMillis() - start;
                    log(String.format(
                            "Load first records from " + listRecords.getRequestURL() + " in %.3f sec.",
                            movingDiff / 1000.0));
                    try {
						processListRecordsResponse(listRecords);
					} catch (Exception e) {
						throw new OaiWrappedException(listRecords.getResponseBytes(), e);
					}
                    fetchedFirst=true;
                    return;
                } catch (OaiWrappedException e) {
                	boolean recoverToken=false;
                	if(e.getCause() instanceof SAXParseException 
                			|| e.getCause() instanceof TransformerException
                			|| e.getCause() instanceof ParserConfigurationException
                			|| e.getCause() instanceof NoSuchFieldException
                			|| e.getCause() instanceof CharConversionException) {
                		recoverToken=true;
                	}else if(e.getCause() instanceof IOException) {
                        System.out.println("IOException block");
                        e.printStackTrace();
                        if (tries > retriesOnError) throw new HarvestException("Failed <" + getRequestURL(baseURL, nextResumptionToken), e);
                        try {
//                            long time = 15000 * (tries * tries * tries);
                            long time = Math.min(340000, 15000 * (tries * tries * tries));
                            log("Failed <" + getRequestURL(baseURL, nextResumptionToken) + " " + e.getClass().getName()+" "+ e.getMessage()+" " +
                                                "> going to retry in: " + time / 1000 + " sec.");
                            Thread.sleep(time);
                        } catch (InterruptedException e1) {
                            // ignore
                        }	
                	} else {
                		recoverToken=true;
                	}
                	if(recoverToken) {
                		if(e.getResponseBytes()==null)
                			throw new HarvestException("Failed <" + getRequestURL(baseURL, from, until, setSpec, metadataPrefix), e);
                        String recoveredResumptionToken = recoverResumptionToken(e.getResponseBytes());
//                      String recoveredResumptionToken = recoverResumptionToken(getRequestURL(baseURL, nextResumptionToken));
                      if(recoveredResumptionToken!=null && !recoveredResumptionToken.isEmpty())
                          nextResumptionToken=recoveredResumptionToken;
                      else {
                          // cannot continue, because we can't find
                          // the next token => next token is null
                          nextResumptionToken = null;
                        throw new HarvestException("Failed <" + getRequestURL(baseURL, from, until, setSpec, metadataPrefix), e);
//                          throw e;
                      }            		
                	}
                }
                    
//                } catch (java.io.CharConversionException | TransformerException | ParserConfigurationException
//                		| NoSuchFieldException | SAXException e) {
//
//                	
//                    String recoveredResumptionToken = recoverResumptionToken(listRecords);
////                    String recoveredResumptionToken = recoverResumptionToken(getRequestURL(baseURL, from, until, setSpec, metadataPrefix));
//                    if(recoveredResumptionToken!=null && !recoveredResumptionToken.isEmpty())
//                        nextResumptionToken=recoveredResumptionToken;
//                    else {
//                        // cannot continue, because we can't find
//                        // the next token => next token is null
//                        nextResumptionToken = null;
//                        throw new HarvestException("Failed <" + getRequestURL(baseURL, from, until, setSpec, metadataPrefix), e);
//                    }
//                } catch (IOException e) {
//                    if (tries > retriesOnError) throw new HarvestException("Failed <" + getRequestURL(baseURL, nextResumptionToken), e);
//                    try {
//                        long time = 15000 * (tries * tries * tries);
//                        log("Failed <" + getRequestURL(baseURL, from, until, setSpec, metadataPrefix) + " " + e.getClass().getName()+" "+ e.getMessage()+" " +
//                                            "> going to retry in: " + time / 1000 + " sec.");
//                        Thread.sleep(time);
//                    } catch (InterruptedException e1) {
//                        // ignore
//                    }
//                } catch (Exception e) {
//                    String recoveredResumptionToken = recoverResumptionToken(listRecords);
////                    String recoveredResumptionToken = recoverResumptionToken(getRequestURL(baseURL, from, until, setSpec, metadataPrefix));
//                    if(recoveredResumptionToken!=null && !recoveredResumptionToken.isEmpty())
//                        nextResumptionToken=recoveredResumptionToken;
//                    else {
//                        // cannot continue, because we can't find
//                        // the next token => next token is null
//                        nextResumptionToken = null;
//                        throw new HarvestException("Failed <" + getRequestURL(baseURL, from, until, setSpec, metadataPrefix), e);
//                    }
//                }
            }
        } finally {
            if(saveNextResumptionTokenIn!=null && nextResumptionToken!=null) {
                try {
                    FileUtils.write(saveNextResumptionTokenIn, nextResumptionToken, StandardCharsets.UTF_8);
                } catch (IOException e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
            }
        }
    }

    protected synchronized void fetchNextIfEmpty() throws HarvestException {
    	if(!fetchedFirst) {
    		fetchFirstRecords();
    		return;
    	}
        try {
            if (records.isEmpty()) {
                if (nextResumptionToken == null || "".equals(nextResumptionToken)) { return; }
                ListRecords resumeListRecordsWithRetry = resumeListRecordsWithRetry();
                processListRecordsResponse(resumeListRecordsWithRetry);
            }
        } catch (HarvestException e) {
            throw e;
        } catch (Throwable e) {
            throw new HarvestException(getRequestURL(baseURL, nextResumptionToken), e);
        }finally {
            if(saveNextResumptionTokenIn!=null && nextResumptionToken!=null) {
                try {
                    FileUtils.write(saveNextResumptionTokenIn, nextResumptionToken, StandardCharsets.UTF_8);
                } catch (IOException e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
            }
        }
    }

    protected void processListRecordsResponse(ListRecords listRecords) throws HarvestException,
            TransformerException, NoSuchFieldException {
        
        NodeList errors = listRecords.getErrors();
        if (errors != null && errors.getLength() > 0) {
            int length = errors.getLength();
            for (int i = 0; i < length; ++i) {
                Node item = errors.item(i);
                Node code = item.getAttributes().getNamedItem("code");
                if (code != null && "noRecordsMatch".equals(code.getNodeValue())) {
                    // only in this case we do not treat it as an error.
                } else {
                	if (item.getNodeValue()==null)
                		throw new HarvestException(listRecords.getRequestURL() + " returned error:" + new String(listRecords.getResponseBytes(), StandardCharsets.UTF_8));
                	else
                		throw new HarvestException(listRecords.getRequestURL() + " returned error:" +
                                               item.getNodeValue());
                }
            }
        } else {
            String xpath = "//ListRecords/record";
            String uri0 = listRecords.getDocument().getDocumentElement().getNamespaceURI();
            if ("http://www.openarchives.org/OAI/2.0/".equals(uri0)) {
                xpath = "//oai20:ListRecords/oai20:record";
        }

            try {
                NodeList recordsNodeList = listRecords.getNodeList(xpath);

                for (int i = 0; i < recordsNodeList.getLength(); i++) {
                    try {
                        Node node = recordsNodeList.item(i);
    
                        OaiPmhRecord record = new OaiPmhRecord(node);
                        records.offer(record);
                    } catch (OutOfMemoryError e1) {
                        log("Insufficient memory to handle OAI record (too extensive metadata?): skipping record"); 
                    }
                }
            } catch (OutOfMemoryError e1) {
                //error processing too large metadata skip it...
                log("Insufficient memory to handle OAI response (too extensive metadata?): Skipping whole OAI response content"); 
            }

            nextResumptionToken = listRecords.getResumptionToken();
            if (StringUtils.isEmpty(nextResumptionToken))
            	nextResumptionToken=null;

            if (this.completeListSize == -1) {
                String listSizeXpath = "/oai20:OAI-PMH/oai20:ListRecords/oai20:resumptionToken/@completeListSize";
                if ("http://www.openarchives.org/OAI/2.0/".equals(uri0))
                    listSizeXpath = "//oai20:ListRecords/oai20:resumptionToken/@completeListSize";
                String sizeStr = listRecords.getSingleString(listSizeXpath);
                if (sizeStr != null && !sizeStr.isEmpty()) try {
                    completeListSize = Integer.parseInt(sizeStr);
                } catch (NumberFormatException e) {
                    // just ignore
                }
            }

        }
    }

    /**
     * Executes a list records request and in case of failure, it waits for some seconds and tries
     * again.
     * 
     * @return a ListRecords
     * @throws Exception
     *             if the maximum number of retries was reached
     */
    protected ListRecords resumeListRecordsWithRetry() throws Exception {
        int tries = 0;
        while (true) {
            tries++;
            ListRecords listRecords = null;
            try {
                long start = System.currentTimeMillis();
                listRecords = new ListRecords(baseURL, nextResumptionToken, saveNextResponseIn);
                movingDiff = (System.currentTimeMillis() - start);
                
                long prevBatchRecords = totalRetrievedRecords - movingCount;
                double currentAverage = (prevBatchRecords * 1000.0) / movingDiff;
                movingAverage = (movingAverage * movingSteps + currentAverage) / (movingSteps + 1.0); 

                log(listRecords.getRequestURL() +
                        String.format(" took %.3f sec. Average: %.3f/sec, (Last m=%.3f/sec, %d, %d)",
                                movingDiff / 1000.0, movingAverage, currentAverage, prevBatchRecords, movingSteps));

                movingCount = totalRetrievedRecords;
                movingSteps++;
                return listRecords;
            } catch (OaiWrappedException e) {
            	boolean recoverToken=false;
            	if(e.getCause() instanceof SAXParseException || e.getCause() instanceof CharConversionException) {
            		recoverToken=true;
            	}else if(e.getCause() instanceof IOException) {
                    System.out.println("IOException block");
                    if (tries > retriesOnError) throw e;
                    try {
//                        long time = 15000 * (tries * tries * tries);
                        long time = Math.min(340000, 15000 * (tries * tries * tries));
                        log("Failed <" + getRequestURL(baseURL, nextResumptionToken) + " " + e.getClass().getName()+" "+ e.getMessage()+" " +
                                            "> going to retry in: " + time / 1000 + " sec.");
                        Thread.sleep(time);
                    } catch (InterruptedException e1) {
                        // ignore
                    }	
            	} else {
            		recoverToken=true;
            	}
            	if(recoverToken) {
            		if(e.getResponseBytes()==null)
            			throw e;
                    String recoveredResumptionToken = recoverResumptionToken(e.getResponseBytes());
//                  String recoveredResumptionToken = recoverResumptionToken(getRequestURL(baseURL, nextResumptionToken));
                  if(recoveredResumptionToken!=null && !recoveredResumptionToken.isEmpty())
                      nextResumptionToken=recoveredResumptionToken;
                  else {
                      // cannot continue, because we can't find
                      // the next token => next token is null
                      nextResumptionToken = null;
                      throw e;
                  }            		
            	}
            }
            
            
//            catch (SAXParseException | java.io. e) {
//                String recoveredResumptionToken = recoverResumptionToken(listRecords);
////                String recoveredResumptionToken = recoverResumptionToken(getRequestURL(baseURL, nextResumptionToken));
//                if(recoveredResumptionToken!=null && !recoveredResumptionToken.isEmpty())
//                    nextResumptionToken=recoveredResumptionToken;
//                else {
//                    // cannot continue, because we can't find
//                    // the next token => next token is null
//                    nextResumptionToken = null;
//                    throw e;
//                }
//            } catch (IOException e) {
//
//            } catch (Exception e) {
//            	String recoveredResumptionToken = recoverResumptionToken(listRecords);
////                String recoveredResumptionToken = recoverResumptionToken(getRequestURL(baseURL, nextResumptionToken));
//                if(recoveredResumptionToken!=null && !recoveredResumptionToken.isEmpty())
//                    nextResumptionToken=recoveredResumptionToken;
//                else {
//                    // cannot continue, because we can't find
//                    // the next token => next token is null
//                    nextResumptionToken = null;
//                    throw e;
//                }
//            }
        }
    }

    
    /**
     * Get the oai:resumptionToken from the response, without using xml parsing 
     * This is used for error recovery
     * @author Nuno Freire (nfreire@gmail.com)
     * @param requestUrl 
     * @return the oai:resumptionToken value
     */
  protected String recoverResumptionToken(byte[] responseBytes) {
        try {
            Pattern resumptionTokenPattern=Pattern.compile("<[^\\s]*resumptionToken[^>]*>([^<]+)</[^\\s]*resumptionToken");
            if(responseBytes==null)
                return null;
            
            String responseString=new String(responseBytes, "UTF-8");
            Matcher matcher = resumptionTokenPattern.matcher(responseString);
            
            if(matcher.find()) {
            	String recovToken = matcher.group(1);
				if(warnHandler!=null)
            		warnHandler.recoveredError(nextResumptionToken, responseString, recovToken);
                return recovToken;
            }
            warnHandler.unrecoveredError(nextResumptionToken, responseString);
            return null;
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
//    protected String recoverResumptionToken(HarvesterVerb verb) {
//        try {
//            Pattern resumptionTokenPattern=Pattern.compile("<[^\\s]*resumptionToken[^>]*>([^<]+)</[^\\s]*resumptionToken");
//            if(verb.getResponseBytes()==null)
//                return null;
//            
//            String responseString=new String(verb.getResponseBytes(), "UTF-8");
//            Matcher matcher = resumptionTokenPattern.matcher(responseString);
//            
//            if(matcher.find()) {
//            	String recovToken = matcher.group(1);
//				if(warnHandler!=null)
//            		warnHandler.recoveredError(nextResumptionToken, responseString, recovToken);
//                return recovToken;
//            }
//            warnHandler.unrecoveredError(nextResumptionToken, responseString);
//            return null;
//        } catch (UnsupportedEncodingException e) {
//            throw new RuntimeException(e.getMessage(), e);
//        }
//    }

    
    
//    /**
//     * Get the oai:resumptionToken from the response, without using xml parsing 
//     * This is used for error recovery
//     * @author Nuno Freire (nfreire@gmail.com)
//     * @param requestUrl 
//     * @return the oai:resumptionToken value
//     */
//    protected String recoverResumptionToken(String requestUrl) {
//        try {
//            Pattern resumptionTokenPattern=Pattern.compile("<[^\\s]*resumptionToken[^>]*>([^<]+)</[^\\s]*resumptionToken");
//
//            HarvesterVerb retryOfRequest=new HarvesterVerb() {
//            };
//            try {
//                retryOfRequest.harvest(getRequestURL(baseURL, nextResumptionToken));
//            } catch (Exception e) {
//                //ignore all exceptions and try to get the resumption token
//            }
//            if(retryOfRequest.getResponseBytes()==null)
//                return null;
//            
//            String responseString=new String(retryOfRequest.getResponseBytes(), "UTF-8");
//            Matcher matcher = resumptionTokenPattern.matcher(responseString);
//            
//            if(matcher.find())
//                return matcher.group(1);
//            return null;
//        } catch (UnsupportedEncodingException e) {
//            throw new RuntimeException(e.getMessage(), e);
//        }
//    }

    
    
    /**
     * @param maxRecordsToRetrieve
     */
    public void setMaxRecordsToHarvest(int maxRecordsToRetrieve) {
        this.maxRecordsToRetrieve = maxRecordsToRetrieve;
    }

    /**
     * @return The total records as reported by the OAI-PMH server on the first ListRecords response
     */
    public int getCompleteListSize() {
        return completeListSize;
    }

    /**
     * Construct the query portion of the http request
     * 
     * @return a String containing the query portion of the http request
     */
    protected static String getRequestURL(String baseURL, String from, String until, String set,
            String metadataPrefix) {
        StringBuffer requestURL = new StringBuffer(baseURL);
        requestURL.append("?verb=ListRecords");
        if (from != null) requestURL.append("&from=").append(from);
        if (until != null) requestURL.append("&until=").append(until);
        if (set != null) requestURL.append("&set=").append(set);
        requestURL.append("&metadataPrefix=").append(metadataPrefix);
        return requestURL.toString();
    }

    /**
     * Construct the query portion of the http request (resumptionToken version)
     * 
     * @param baseURL
     * @param resumptionToken
     * @return
     */
    protected static String getRequestURL(String baseURL, String resumptionToken) {
        StringBuffer requestURL = new StringBuffer(baseURL);
        requestURL.append("?verb=ListRecords");
        try {
            requestURL.append("&resumptionToken=").append(resumptionToken == null ? "" : 
                    URLEncoder.encode(resumptionToken, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Could not encode '" + resumptionToken == null ? "" : resumptionToken + "' into UTF-8!", e);
        }
        return requestURL.toString();
    }
    
    
    /**
     * @param message
     */
    protected void log(String message) {
        System.out.println(message);
    }

    public File getSaveNextResumptionTokenIn() {
        return saveNextResumptionTokenIn;
    }

    public void setSaveNextResumptionTokenIn(File saveNextResumptionTokenIn) {
        this.saveNextResumptionTokenIn = saveNextResumptionTokenIn;
    }

    public File getSaveNextResponseIn() {
        return saveNextResponseIn;
    }

    public void setSaveNextResponseIn(File saveNextResponseIn) {
        this.saveNextResponseIn = saveNextResponseIn;
    }

	public String getLastResumptionToken() {
		return nextResumptionToken;
	}

	public void setWarnHandler(WarnHandler warnHandler) {
		this.warnHandler = warnHandler;
	}

	public void setRetriesOnError(int retriesOnError) {
		this.retriesOnError = retriesOnError;
	}
}
