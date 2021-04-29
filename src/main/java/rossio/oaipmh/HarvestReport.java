package rossio.oaipmh;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.lang3.exception.ExceptionUtils;

public class HarvestReport {
	int recordCount;
	int recordUpdatedCount;
	int deletedRecordCount;
	int deletedRecordExistedCount;
	HashMap<String, String> errorRecords=new HashMap<String, String>();
	String failureCause;
	Date harvestStart=new Date();
	String resumptionTokenOfLastCommit=null;
	List<String> warnings=new ArrayList<String>();

	public int incRecord() {
		recordCount++;
		return recordCount;
	}
	public int incRecordUpdated() {
		recordUpdatedCount++;
		return recordUpdatedCount;
	}

	public int incDeletedRecord() {
		deletedRecordCount++;
		return deletedRecordCount;
	}
	
	public int incDeletedRecordExisting() {
		deletedRecordExistedCount++;
		return deletedRecordExistedCount;
	}

	public int addErrorOnRecord(String recId, String errorMessage) {
		errorRecords.put(recId, errorMessage);
		return errorRecords.size();
	}
	
	public void failure(String causeErrorMessage) {
		failureCause=causeErrorMessage;
	}
	public void failure(String causeErrorMessage, Exception e) {
		failureCause=causeErrorMessage;
		if(e!=null)
			failureCause+="\n"+ExceptionUtils.getStackTrace(e);
	}
	
	public boolean isSuccessful() {
		return failureCause==null;
	}

	public int getRecordCount() {
		return recordCount;
	}

	public int getDeletedRecordCount() {
		return deletedRecordCount;
	}

	public HashMap<String, String> getErrorRecords() {
		return errorRecords;
	}

	public String getFailureCause() {
		return failureCause;
	}

	public String toLogString() {
		String dateStartStr=new SimpleDateFormat("yyyy-MM-dd HH:mm").format(harvestStart);
		dateStartStr+="\n";
		
		String result="";
		if(!isSuccessful())
			result="FAILURE\nHarvest started at:"+dateStartStr+"\nFail cause: "+getFailureCause();
		else {
			String summary=dateStartStr+"Created Record count: "+(getRecordCount()-getRecordUpdatedCount())+"\n";
			summary+="Updated record count: "+getRecordUpdatedCount()+"\n";
			summary+="Deleted record count: "+getDeletedRecordCount()+"\n";
			summary+="Deleted record from repository count: "+getDeletedRecordExistedCount()+"\n";
			if(!getErrorRecords().isEmpty()) {
				summary+="Error on record count: "+getErrorRecords().size()+"\n";
				for(Entry<String, String> error: getErrorRecords().entrySet())
					summary+=error.getKey() +" - "+error.getValue()+"\n";
			}
			if(!getWarnings().isEmpty()) {
				summary+="Warnings: "+getWarnings().size()+"\n";
				for(String warn: getWarnings())
					summary+=warn+"\n";
			}
			if (getErrorRecords().isEmpty())
				result="SUCCESS\n"+summary;
			else if (! getErrorRecords().isEmpty() && getRecordCount()==0) {
				result="FAILURE\n"+summary;
			} else 
				result="SUCCESS (PARTIAL)\n"+summary;
		}
		return result;
	}

	public boolean isFailure() {
		return failureCause!=null;
	}

	public String getResumptionTokenOfLastCommit() {
		return resumptionTokenOfLastCommit;
	}

	public void setResumptionTokenOfLastCommit(String resumptionTokenOfLastCommit) {
		this.resumptionTokenOfLastCommit = resumptionTokenOfLastCommit;
	}

	public void addWarn(String string) {
	}

	public List<String> getWarnings() {
		return warnings;
	}
	public int getRecordUpdatedCount() {
		return recordUpdatedCount;
	}
	public int getDeletedRecordExistedCount() {
		return deletedRecordExistedCount;
	}
	
}
