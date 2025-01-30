package rossio.ingest.solr;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFWriter;
import org.apache.solr.client.solrj.SolrServerException;
import org.w3c.dom.Element;

import rossio.ingest.solr.manager.Logger;
import rossio.ingest.solr.manager.MetadataSource;
import rossio.ingest.solr.manager.MetadataSources;
import rossio.oaipmh.HarvestException;
import rossio.oaipmh.HarvestReport;
import rossio.oaipmh.OaiPmhRecord;
import rossio.oaipmh.OaipmhHarvest;
import rossio.oaipmh.OaipmhHarvestWithHandler;
import rossio.oaipmh.OaipmhHarvestWithHandler.Handler;
import rossio.util.RdfUtil;
import rossio.util.XmlUtil;

public class HarvestOaiSourceIntoSolr {
	RepositoryWithSolr harvestTo;
	
//	String baseUrl;
//	String set;
//	String metadataPrefix;
//	Date lastHarvestTimestamp;
	MetadataSource source;
	String resumptionToken;
	
//	String sourceId;
//	String dataProviderUri;

	int commitInterval;

	HarvestReport report;
	
//	public HarvestOaiSourceIntoSolrWithHandler(String sourceId, String dataProviderUri, String baseUrl, String set, String metadataPrefix, Date lastHarvestTimestamp, RepositoryWithSolr harvestTo) {
//		super();
//		this.harvestTo = harvestTo;
//		this.baseUrl = baseUrl;
//		this.set = set;
//		this.metadataPrefix = metadataPrefix;
//		this.sourceId = sourceId;
//		this.dataProviderUri = dataProviderUri;
//		this.lastHarvestTimestamp = lastHarvestTimestamp;
//	}
	
	public HarvestOaiSourceIntoSolr(MetadataSource src, RepositoryWithSolr repository) {
		this.harvestTo = repository;
		this.source=src;
	}

	public HarvestReport run(Logger log, MetadataSources sources, MetadataSource src) throws HarvestException {
		return run(null, log, sources, src);
	}
	public HarvestReport run(Integer maxRecords, Logger log, MetadataSources sources, MetadataSource src) {
		int maximumRetries=3;
        int retry=0;
        report=null;

        
//        try {
//			harvestTo.removeAllFrom(source.getSourceId());
//		} catch (SolrServerException e2) {
//			e2.printStackTrace();
//		} catch (IOException e2) {
//			e2.printStackTrace();
//		}
        
        while (retry>=0 && retry<=maximumRetries && (report==null || !report.isFailure())) {
             report=new HarvestReport();
			 report.setResumptionTokenOfLastCommit(resumptionToken);
			 try {
				 OaipmhHarvestWithHandler harvest;
				 if(!StringUtils.isEmpty(resumptionToken))
					 harvest=new OaipmhHarvestWithHandler(source.baseUrl, resumptionToken);
				 else if(source.lastHarvestTimestamp==null)
					 harvest=new OaipmhHarvestWithHandler(source.baseUrl, source.metadataPrefix, source.set);
				 else {
					 SimpleDateFormat format=new SimpleDateFormat("yyyy-MM-dd");
					 harvest=new OaipmhHarvestWithHandler(source.baseUrl, format.format(source.lastHarvestTimestamp), null, source.metadataPrefix, source.set);
				 }
	             
				 harvest.run(new Handler() {
					@Override
					public void unrecoveredError(String lostResumptionToken, String lostResponse) {
					}
					
					@Override
					public void recoveredError(String lostResumptionToken, String lostResponse, String recoveredResumptionToken) {
						report.addWarn("Lost results in token \""+lostResumptionToken+"\"");
					}
					
					int recordCount=0;
					@Override
					public boolean handle(OaiPmhRecord record) {
						try {
							recordCount++;
							handleRecord(record);
							
//							System.out.println(record.getIdentifier());
//							System.out.println( XmlUtil.writeDomToString(record.getMetadata()) );
							
							if(maxRecords != null && maxRecords > 0 && report.getRecordCount()>=maxRecords)
								return false;
//							if(report.getRecordCount()>990 && report.getRecordCount()<1100) {
//								log.log("DEBUG: "+(commitInterval>0 && beforeCount!=report.getRecordCount() && 
//										report.getRecordCount() % commitInterval == 0));
//							}
							if(commitInterval>0 && recordCount % commitInterval == 0) {
//								log.log("DEBUG_ commiting"); 
								harvestTo.commit();
								report.setResumptionTokenOfLastCommit(harvest.getLastResumptionToken());
						    	src.resumptionToken=harvest.getLastResumptionToken();
						    	sources.save();
								resumptionToken=harvest.getLastResumptionToken();
							}
						} catch (Exception e) {
							e.printStackTrace();
							errorOnRecord(record, e);
						}
						return true;
					}
				});
	             retry=-1;
	             harvestTo.end();
	         } catch (Exception e) {
	        	 try {
					 harvestTo.abort();
		             retry++;
		             if(retry>=maximumRetries) {
		            	 report.failure("Harvest failed. Cause:", e);
		             } else {
		            	 log.log("WARN: Harvest failed. Retrying.\n Cause: "+ExceptionUtils.getStackTrace(e));
		             	 harvestTo.restart();
		             }
	        	 } catch (IOException e1) {
	        		 report.failure("Harvest failed. Cause:", e);
	        	 } catch (SolrServerException e1) {
	        		 report.failure("Harvest failed. Cause:", e);
				}
	         }
        }
        return report;
	}

	private void errorOnRecord(OaiPmhRecord r, Exception e) {
		report.addErrorOnRecord(r.getIdentifier(), e.getMessage());
	}

	private void handleRecord(OaiPmhRecord r) throws IOException, SolrServerException {
		if(!r.isDeleted()) {
			report.incRecord();
			String uuid=harvestTo.getRecordUuid(source.getSourceId(), r.getIdentifier());
			if(uuid==null)
				uuid = UUID.randomUUID().toString();
			else
				report.incRecordUpdated();
			Element metadata = r.getMetadata();
			
			if(metadata==null) {
				System.out.println("Empty metadata: "+r.getIdentifier());
				return;
			}			
			if(source.preprocessor!=null) {
				Model mdRdf=source.preprocessor.preprocess(uuid, source.getSourceId(),source.dataProvider, metadata);				
				harvestTo.addItem(uuid, source.getSourceId(), r.getIdentifier(), RdfUtil.serializeToRdfRift(mdRdf));
			} else
				harvestTo.addItem(uuid, source.getSourceId(), r.getIdentifier(), serializeToRossioRdfRift(uuid, metadata));
		} else {
			String uuid=harvestTo.getRecordUuid(source.getSourceId(), r.getIdentifier());
			if(uuid!=null) {
				harvestTo.delete(uuid);
				report.incDeletedRecordExisting();
			}
			report.incDeletedRecord();
		}
	}

	private byte[] serializeToRossioRdfRift(String uuid, Element metadata) {
		Model m=RossioRecord.fromOaidcToRossio(uuid, source.getSourceId(), source.dataProvider, metadata);
//		RdfUtil.printOutRdf(m);
		return RdfUtil.serializeToRdfRift(m);
	}

	public void setCommitInterval(int commitInterval) {
		this.commitInterval=commitInterval;
	}

	public void resumeWithToken(String resumptionToken) {
		this.resumptionToken = resumptionToken;
	}
	
}
