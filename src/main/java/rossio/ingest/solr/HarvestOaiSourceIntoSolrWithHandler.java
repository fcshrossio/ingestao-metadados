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
import rossio.oaipmh.HarvestException;
import rossio.oaipmh.HarvestReport;
import rossio.oaipmh.OaiPmhRecord;
import rossio.oaipmh.OaipmhHarvest;
import rossio.oaipmh.OaipmhHarvestWithHandler;
import rossio.oaipmh.OaipmhHarvestWithHandler.Handler;

public class HarvestOaiSourceIntoSolrWithHandler {
	RepositoryWithSolr harvestTo;
	
	String baseUrl;
	String set;
	String metadataPrefix;
	String resumptionToken;
	Date lastHarvestTimestamp;
	
	String sourceId;
	String dataProviderUri;

	int commitInterval;

	HarvestReport report;
	
	public HarvestOaiSourceIntoSolrWithHandler(String sourceId, String dataProviderUri, String baseUrl, String set, String metadataPrefix, Date lastHarvestTimestamp, RepositoryWithSolr harvestTo) {
		super();
		this.harvestTo = harvestTo;
		this.baseUrl = baseUrl;
		this.set = set;
		this.metadataPrefix = metadataPrefix;
		this.sourceId = sourceId;
		this.dataProviderUri = dataProviderUri;
		this.lastHarvestTimestamp = lastHarvestTimestamp;
	}
	
	public HarvestReport run(Logger log) throws HarvestException {
		return run(null, log);
	}
	public HarvestReport run(Integer maxRecords, Logger log) {
		int maximumRetries=3;
        int retry=0;
        report=null;

        while (retry>=0 && retry<=maximumRetries && (report==null || !report.isFailure())) {
             report=new HarvestReport();
			 report.setResumptionTokenOfLastCommit(resumptionToken);
			 try {
				 OaipmhHarvestWithHandler harvest;
				 if(!StringUtils.isEmpty(resumptionToken))
					 harvest=new OaipmhHarvestWithHandler(baseUrl, resumptionToken);
				 else if(lastHarvestTimestamp==null)
					 harvest=new OaipmhHarvestWithHandler(baseUrl, metadataPrefix, set);
				 else {
					 SimpleDateFormat format=new SimpleDateFormat("yyyy-MM-dd");
					 harvest=new OaipmhHarvestWithHandler(baseUrl, format.format(lastHarvestTimestamp), null, metadataPrefix, set);
				 }
	             
				 harvest.run(new Handler() {
					@Override
					public void unrecoveredError(String lostResumptionToken, String lostResponse) {
					}
					
					@Override
					public void recoveredError(String lostResumptionToken, String lostResponse, String recoveredResumptionToken) {
						report.addWarn("Lost results in token \""+lostResumptionToken+"\"");
					}
					
					@Override
					public boolean handle(OaiPmhRecord record) {
						try {
							int beforeCount = report.getRecordCount();
							handleRecord(record);
							if(maxRecords != null && maxRecords > 0 && beforeCount>=maxRecords)
								return false;
							if(commitInterval>0 && beforeCount!=report.getRecordCount() && 
									report.getRecordCount() % commitInterval == 0) {
								harvestTo.commit();
								report.setResumptionTokenOfLastCommit(harvest.getLastResumptionToken());
								resumptionToken=harvest.getLastResumptionToken();
							}
						} catch (Exception e) {
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
			String uuid=harvestTo.getRecordUuid(sourceId, r.getIdentifier());
			if(uuid==null)
				uuid = UUID.randomUUID().toString();
			else
				report.incRecordUpdated();
			harvestTo.addItem(uuid, sourceId, r.getIdentifier(), serializeToRossioRdfRift(uuid, r.getMetadata()));
		} else {
			String uuid=harvestTo.getRecordUuid(sourceId, r.getIdentifier());
			if(uuid!=null) {
				harvestTo.delete(uuid);
				report.incDeletedRecordExisting();
			}
			report.incDeletedRecord();
		}
	}

	private byte[] serializeToRossioRdfRift(String uuid, Element metadata) {
		Model m=RossioRecord.fromOaidcToRossio(uuid, sourceId, dataProviderUri, metadata);
//		RdfUtil.printOutRdf(m);
		
		RDFWriter writer = RDFWriter.create().lang(Lang.RDFTHRIFT).source(m.getGraph()).build();
		ByteArrayOutputStream outstream=new ByteArrayOutputStream();
		writer.output(outstream);
		return outstream.toByteArray();
	}

	public void setCommitInterval(int commitInterval) {
		this.commitInterval=commitInterval;
	}

	public void resumeWithToken(String resumptionToken) {
		this.resumptionToken = resumptionToken;
	}
	
}
