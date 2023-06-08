package rossio.ingest.solr.manager;

import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;

import rossio.ingest.solr.HarvestFileSourceIntoSolr;
import rossio.ingest.solr.HarvestOaiSourceIntoSolr;
import rossio.ingest.solr.RepositoryWithSolr;
import rossio.ingest.solr.manager.MetadataSource.IngestMethod;
import rossio.ingest.solr.manager.TaskThread.Task;
import rossio.oaipmh.HarvestReport;

public class ManagerOfHarvest implements Task{
	RepositoryWithSolr repository;
	MetadataSources oaiSources;
	Logger log;
	int commitInterval;
	String title=this.getClass().getCanonicalName();
	String currentSource="";
	
	Date lastRun=null;
	boolean stopWhenPossible=false;
	
	public ManagerOfHarvest(RepositoryWithSolr repository, MetadataSources oaiSources, Logger log, int commitInterval) {
		super();
		this.repository = repository;
		this.oaiSources = oaiSources;
		this.log = log;
		this.commitInterval = commitInterval;
	}

	public void run() throws Exception {
		TOP: while(!stopWhenPossible) {
			if(!runNow()) {
				log.log("Harvester sleeping 10 minutes");	
				for(int i=0 ; i<60 && !stopWhenPossible ; i++) { //10 minutes sleep
					Thread.sleep(1000*10);
				}
			} else {
				lastRun=new Date();
		    	List<MetadataSource> sourcesToHarvest = oaiSources.getSourcesToHarvest();
		    	if(sourcesToHarvest.isEmpty())
		    		log.log("No sources need harvesting");
				for(MetadataSource src: sourcesToHarvest) {
					if(stopWhenPossible) 
						break TOP;
					currentSource=src.getSourceId();
		    		log.log("Start harvesting: "+src.getSourceId());
		    		
		    		HarvestReport report = null;
		    		Date startOfharvest=null;
		    		if(src.ingestMethod==IngestMethod.OAIPMH) {
			//			HarvestOaiSourceIntoSolr harvest=new HarvestOaiSourceIntoSolr(src.getSourceId(), src.dataProvider, src.baseUrl, src.set, src.metadataPrefix, repository);
	//					HarvestOaiSourceIntoSolrWithHandler harvest=new HarvestOaiSourceIntoSolrWithHandler(src.getSourceId(), src.dataProvider, src.baseUrl, src.set, src.metadataPrefix, src.lastHarvestTimestamp, repository);
			    		HarvestOaiSourceIntoSolr harvest=new HarvestOaiSourceIntoSolr(src, repository);
						harvest.setCommitInterval(commitInterval);
						if(!StringUtils.isEmpty(src.resumptionToken)) {
							harvest.resumeWithToken(src.resumptionToken);
							log.log("...resuming with token: "+src.resumptionToken);				
						}else if (src.status!=null && src.status==TaskStatus.CLEAR)
				    		repository.removeAllFrom(src.getSourceId());
			
						startOfharvest=new Date();
						report = harvest.run(log, oaiSources, src);
		    		} else if(src.ingestMethod==IngestMethod.File) {
		    			HarvestFileSourceIntoSolr harvest=new HarvestFileSourceIntoSolr(src, repository);
		    			harvest.setCommitInterval(commitInterval);
		    			repository.removeAllFrom(src.getSourceId());		    			
		    			startOfharvest=new Date();
		    			report = harvest.run(log, oaiSources, src);
		    		}

		    		
		    		String result=report.toLogString();
		
					if(report.isSuccessful())
						src.lastHarvestTimestamp=DateUtils.addHours(startOfharvest, -12);
						
					if(!report.isSuccessful() && report.getResumptionTokenOfLastCommit()!=null)
						src.resumptionToken=report.getResumptionTokenOfLastCommit();
					else
						src.resumptionToken=null;
						
					log.log("End: "+src.getSourceId());
			    	log.log(result);
			    	src.updateStatus(result);
			    	oaiSources.save();
			    	System.out.println("Harvest concluded: "+src.getSourceId());
		    	}
			}
		}
		log.log("Exiting harvester");
	}

	private boolean runNow() {
		if(lastRun==null) 
			return true;
		Date nextRun = DateUtils.addHours(lastRun, 1);
		return new Date().after(nextRun);
	}

	@Override
	public String getTitle() {
		return title+" - "+currentSource;
	}

	@Override
	public void stopWhenPossible() {
		stopWhenPossible=true;
	}
}
