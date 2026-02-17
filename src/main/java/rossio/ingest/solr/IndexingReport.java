package rossio.ingest.solr;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;

public class IndexingReport {
	int recordCount;
	int deletedCount;
	HashMap<String, String> errorRecords=new HashMap<String, String>();
	String failureCause;
	Date indexingStart=new Date();
	long indexingTimeMs=-1;
	
	
	public int incRecord() {
		recordCount++;
		return recordCount;
	}

	public int addErrorOnRecord(String recId, String errorMessage) {
		errorRecords.put(recId, errorMessage);
		return errorRecords.size();
	}
	
	public void failure(String causeErrorMessage) {
		failureCause=causeErrorMessage;
	}
	public void failure(Exception e) {
		failure(e.getMessage(), e);
	}
	public void failure(String causeErrorMessage, Exception e) {
		failureCause=causeErrorMessage+"\n"+ExceptionUtils.getStackTrace(e);
	}
	public void finish() {
		indexingTimeMs=new Date().getTime()- indexingStart.getTime(); 
	}
	
	public boolean isSuccessful() {
		return failureCause==null;
	}

	public int getRecordCount() {
		return recordCount;
	}


	public HashMap<String, String> getErrorRecords() {
		return errorRecords;
	}

	public String getFailureCause() {
		return failureCause;
	}

	public String toLogString() {
//		String dateStartStr=new SimpleDateFormat("yyyy-MM-dd:HH:mm").format(indexingStart);
		String dateStartStr="";
		if(indexingTimeMs>0) {
//			dateStartStr+=" Duration "+ DurationFormatUtils.formatDuration(indexingTimeMs, "H:mm:ss", true);
			dateStartStr+="Duration "+ DurationFormatUtils.formatDuration(indexingTimeMs, "H:mm:ss", true);
			dateStartStr+=" Rate "+ ((double)recordCount / ((double)indexingTimeMs/1000))+" recs./sec";
		}
		dateStartStr+=" ";
//		dateStartStr+="\n";
		
		String result="";
		if(!isSuccessful())
			result="FAILURE\n"+dateStartStr+getFailureCause();
		else {
			String summary=dateStartStr+"Record count: "+getRecordCount()+"\n";
			summary+="Deleted count: "+getDeletedCount()+"\n";
			if(!getErrorRecords().isEmpty()) {
				summary+="Error on record count: "+getErrorRecords().size()+"\n";
				for(Entry<String, String> error: getErrorRecords().entrySet())
					summary+=error.getKey() +" - "+error.getValue()+"\n";
			}
			if (getErrorRecords().isEmpty())
				result="SUCCESS: "+summary;
			else if (! getErrorRecords().isEmpty() && getRecordCount()==0) {
				result="FAILURE: "+summary;
			} else 
				result="PARTIAL SUCCESS: "+summary;
		}
		return result;
	}
	
	/**
   * @return
   */
  private int getDeletedCount() {
    return deletedCount;
  }

  public String toLogStringIntermediate() {
		String dateStartStr="";
		long indTime=new Date().getTime()- indexingStart.getTime(); 
		if(indTime>0) {
//			dateStartStr+=" Duration "+ DurationFormatUtils.formatDuration(indexingTimeMs, "H:mm:ss", true);
			dateStartStr+="Duration "+ DurationFormatUtils.formatDuration(indTime, "H:mm:ss", true);
			dateStartStr+=" Rate "+ ((double)recordCount / ((double)indTime/1000))+" recs./sec";
		}
		dateStartStr+= "Record count: "+getRecordCount() + "  Deleted count: "+getDeletedCount();
		return dateStartStr;
	}

  /**
   * 
   */
  public void incDeleted() {
    deletedCount++;
  }
	
}
