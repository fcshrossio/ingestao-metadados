package rossio.ingest.solr.old;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Seq;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFWriter;
import org.apache.solr.client.solrj.SolrServerException;
import org.w3c.dom.Element;

import rossio.data.models.DcTerms;
import rossio.data.models.Edm;
import rossio.data.models.Ore;
import rossio.data.models.Rdf;
import rossio.data.models.Rossio;
import rossio.ingest.solr.manager.Logger;
import rossio.oaipmh.HarvestException;
import rossio.oaipmh.HarvestReport;
import rossio.oaipmh.OaiPmhRecord;
import rossio.oaipmh.OaipmhHarvest;
import rossio.util.RdfUtil.Jena;
import rossio.util.MapOfLists;
import rossio.util.RdfUtil;
import rossio.util.XmlUtil;

public class HarvestOaiSourceIntoSolr {
//	RepositoryWithSolr harvestTo;
//	
//	String baseUrl;
//	String set;
//	String metadataPrefix;
//	String resumptionToken;
//	
//	String sourceId;
//	String dataProviderUri;
//
//	int commitInterval;
//
//	HarvestReport report;
//	
//	public HarvestOaiSourceIntoSolr(String sourceId, String dataProviderUri, String baseUrl, String set, String metadataPrefix, RepositoryWithSolr harvestTo) {
//		super();
//		this.harvestTo = harvestTo;
//		this.baseUrl = baseUrl;
//		this.set = set;
//		this.metadataPrefix = metadataPrefix;
//		this.sourceId = sourceId;
//		this.dataProviderUri = dataProviderUri;
//	}
//	
//	public HarvestReport run(Logger log) throws HarvestException {
//		return run(null, log);
//	}
//	public HarvestReport run(Integer maxRecords, Logger log) {
//		int maximumRetries=3;
//        int retry=0;
//        report=new HarvestReport();
//
//        while (retry>=0 && retry<=maximumRetries && !report.isFailure()) {
//			 try {
//				 OaipmhHarvest harvest;
//				 if(resumptionToken!=null)
//					 harvest=new OaipmhHarvest(baseUrl, resumptionToken);
//				 else
//					 harvest=new OaipmhHarvest(baseUrl, metadataPrefix, set);
//	             
//	             while (harvest.hasNext()) {
//	                 OaiPmhRecord r=harvest.next();
//	                 try {
//	                	 int beforeCount = report.getRecordCount();
//	                     handleRecord(r);
//	                     if(maxRecords != null && maxRecords > 0 && beforeCount>=maxRecords)
//	                    	 break;
//	                     if(commitInterval>0 && beforeCount!=report.getRecordCount() && 
//	                    		 report.getRecordCount() % commitInterval == 0) {
//	                    	 harvestTo.commit();
//	                    	 report.setResumptionTokenOfLastCommit(harvest.getLastResumptionToken());
//	                     }
//	                 } catch (Exception e) {
//	                     errorOnRecord(r, e);
//	                 }
//	             }
//	             retry=-1;
//	             harvestTo.end();
//	         } catch (Exception e) {
//	        	 try {
//					 harvestTo.abort();
//		             retry++;
//		             if(retry>=maximumRetries) {
//		            	 report.failure("Harvest failed. Cause:", e);
//		             } else {
//		            	 log.log("WARN: Harvest failed. Retrying.\n Cause: "+ExceptionUtils.getStackTrace(e));
//		             	 harvestTo.restart();
//		             }
//	        	 } catch (IOException e1) {
//	        		 report.failure("Harvest failed. Cause:", e);
//	        	 } catch (SolrServerException e1) {
//	        		 report.failure("Harvest failed. Cause:", e);
//				}
//	         }
//        }
//        return report;
//	}
//
//	private void errorOnRecord(OaiPmhRecord r, Exception e) {
//		report.addErrorOnRecord(r.getIdentifier(), e.getMessage());
//	}
//
//	private void handleRecord(OaiPmhRecord r) throws IOException, SolrServerException {
//		if(!r.isDeleted()) {
//			report.incRecord();
//			String uuid = UUID.randomUUID().toString();
//			harvestTo.addItem(uuid, sourceId, r.getIdentifier(), serializeToRossioRdfRift(uuid, r.getMetadata()));
//		} else
//			report.incDeletedRecord();
//	}
//
//	private byte[] serializeToRossioRdfRift(String uuid, Element metadata) {
//		Model m=RossioRecord.fromOaidcToRossio(uuid, sourceId, dataProviderUri, metadata);
////		RdfUtil.printOutRdf(m);
//		
//		RDFWriter writer = RDFWriter.create().lang(Lang.RDFTHRIFT).source(m.getGraph()).build();
//		ByteArrayOutputStream outstream=new ByteArrayOutputStream();
//		writer.output(outstream);
//		return outstream.toByteArray();
//	}
//
//	public void setCommitInterval(int commitInterval) {
//		this.commitInterval=commitInterval;
//	}
//
//	public void resumeWithToken(String resumptionToken) {
//		this.resumptionToken = resumptionToken;
//	}
	
}
