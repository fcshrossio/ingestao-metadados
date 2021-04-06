package rossio.ingest.dspace.old;

import java.io.IOException;
import java.util.Calendar;

import org.w3c.dom.Element;

import rossio.ingest.datasets.dspace.SimpleArchive;
import rossio.oaipmh.HarvestException;
import rossio.oaipmh.HarvestReport;
import rossio.oaipmh.OaiPmhRecord;
import rossio.oaipmh.OaipmhHarvest;

public class HarvestOaiSourceIntoSimpleArchive {
	SimpleArchive harvestTo;
	String baseUrl;
	String set;
	String metadataPrefix;

	HarvestReport report;
	
	public HarvestOaiSourceIntoSimpleArchive(String baseUrl, String set, String metadataPrefix, SimpleArchive harvestTo) {
		super();
		this.harvestTo = harvestTo;
		this.baseUrl = baseUrl;
		this.set = set;
		this.metadataPrefix = metadataPrefix;
	}
	
	public HarvestReport run() throws HarvestException {
		return run(null);
	}
	public HarvestReport run(Integer maxRecords) throws HarvestException {
		int maximumRetries=3;
        int retry=0;
        report=new HarvestReport();

        while (retry>=0 && retry<=maximumRetries) {
			 try {
				 OaipmhHarvest harvest=new OaipmhHarvest(baseUrl, metadataPrefix, set);
	             
	             while (harvest.hasNext()) {
	                 OaiPmhRecord r=harvest.next();
	                 try {
	                     handleRecord(r);
	                     if(maxRecords != null && maxRecords > 0 && report.getRecordCount()>=maxRecords)
	                    	 break;
	                 } catch (Exception e) {
	                     errorOnRecord(r, e);
	                 }
	             }
	             retry=-1;
	             harvestTo.close();
	         } catch (Exception e) {
	        	 try {
					harvestTo.abort();
		             retry++;
		             if(retry>=maximumRetries) {
	            		 throw new HarvestException("Harvest failed. Cause:", e);
		             } else 
		             	harvestTo.restart();
	        	 } catch (IOException e1) {
	        		 throw new HarvestException("Harvest failed. Cause:", e);
	        	 }
	         }
        }
        return report;
	}

	private void errorOnRecord(OaiPmhRecord r, Exception e) {
		report.addErrorOnRecord(r.getIdentifier(), e.getMessage());
	}

	private void handleRecord(OaiPmhRecord r) throws IOException {
		if(!r.isDeleted()) {
			report.incRecord();
			harvestTo.addItem(r.getIdentifier(), OaiDcToDspaceDcConverter.convert(r.getMetadata()));
		} else
			report.incDeletedRecord();
	}
	
}
