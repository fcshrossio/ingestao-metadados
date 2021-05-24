package rossio.ingest.solr.manager;

import java.util.Date;
import java.util.List;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.time.DateUtils;

import rossio.ingest.solr.Indexer;
import rossio.ingest.solr.IndexingReport;
import rossio.ingest.solr.RepositoryWithSolr;
import rossio.ingest.solr.manager.TaskThread.Task;

public class ManagerOfIndexing  implements Task {

	RepositoryWithSolr repository;
	Indexer indexer;
//	OaiSources oaiSources;
	OaiSourcesIndexStatus oaiSourcesIndexStatus;
	Logger log;
	int commitInterval;
	String title=this.getClass().getCanonicalName();
	String currentSource="";

	Date lastRun=null;
	boolean stopWhenPossible=false;
	
	public ManagerOfIndexing(RepositoryWithSolr repository, Indexer indexer, OaiSourcesIndexStatus oaiSources, Logger log, int commitInterval) {
		super();
		this.repository = repository;
		this.indexer = indexer;
		this.oaiSourcesIndexStatus = oaiSources;
		this.log = log;
		this.commitInterval = commitInterval;
	}

	public void run() throws Exception {
		TOP: while(!stopWhenPossible) {
			if(!runNow()) {
				log.log("Indexer sleeping 10 minutes");				
				for(int i=0 ; i<60 && !stopWhenPossible ; i++) { //10 minutes sleep
					Thread.sleep(1000*10);
				}
			} else {
				lastRun=new Date();
				oaiSourcesIndexStatus.refresh();
		    	List<OaiSourceIndexStatus> sourcesToIndex = oaiSourcesIndexStatus.getSourcesToIndex();
		    	log.log("Will index "+sourcesToIndex.size()+" sources");
				for(OaiSourceIndexStatus src: sourcesToIndex) {
					if(stopWhenPossible) 
						break TOP;
					currentSource=src.oaiSource.getSourceId();
		    		log.log("Start indexing: "+src.oaiSource.getSourceId());
		    		System.out.println("Start indexing: "+src.oaiSource.getSourceId());
		
		    		String result=null;
			    	try {
			    		IndexingReport report=indexer.indexSourceFromRepository(src.getSourceId(), repository, log);
						result=report.toLogString();
					} catch (Exception e) {
						result="FAILURE\nstackTrace:\n"+ExceptionUtils.getStackTrace(e);
					}
		
					log.log("End indexing: "+src.getSourceId());
			    	log.log(result);
			    	src.updateStatus(result);
			    	oaiSourcesIndexStatus.save();
			    	System.out.println("Indexing concluded: "+src.getSourceId());
		    	}
			}
		}
		log.log("Exiting indexer");
	}
	@Override
	public String getTitle() {
		return title+" - "+currentSource;
	}


	private boolean runNow() {
		if(lastRun==null) 
			return true;
		Date nextRun = DateUtils.addHours(lastRun, 1);
		return new Date().after(nextRun);
	}
	
	@Override
	public void stopWhenPossible() {
		stopWhenPossible=true;
	}
}
