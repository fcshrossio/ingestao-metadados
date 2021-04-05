package rossio.ingestao.dspace;

import java.io.IOException;
import java.util.Calendar;

import org.w3c.dom.Element;

import rossio.oaipmh.HarvestException;
import rossio.oaipmh.OaiPmhRecord;
import rossio.oaipmh.OaipmhHarvest;

public class HarvestOaiSourceIntoSimpleArchive {
	SimpleArchive harvestTo;
	String baseUrl;
	String set;

	public HarvestOaiSourceIntoSimpleArchive(String baseUrl, String set,SimpleArchive harvestTo) {
		super();
		this.harvestTo = harvestTo;
		this.baseUrl = baseUrl;
		this.set = set;
	}
	
	public void run(Integer maxRecords) throws HarvestException {
		int maximumRetries=3;
        int retry=0;

        int recCounter=0;

        while (retry>=0 && retry<=maximumRetries) {
			 try {
				 OaipmhHarvest harvest=new OaipmhHarvest(baseUrl, "oai_dc", set);
	             
	             while (harvest.hasNext()) {
	                 OaiPmhRecord r=harvest.next();
	                 try {
	                     handleRecord(r);
	                     if(!r.isDeleted())
	                    	 recCounter++;
	                     if(maxRecords != null && maxRecords > 0 && recCounter>=maxRecords)
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
	}

	private void errorOnRecord(OaiPmhRecord r, Exception e) {
		e.printStackTrace();
	}

	private void handleRecord(OaiPmhRecord r) throws IOException {
		if(!r.isDeleted())
			harvestTo.addItem(r.getIdentifier(), OaiDcToDspaceDcConverter.convert(r.getMetadata()));
	}
	
}
