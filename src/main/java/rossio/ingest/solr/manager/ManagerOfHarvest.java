package rossio.ingest.solr.manager;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.logging.LogManager;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.xerial.snappy.OSInfo;

import com.ctc.wstx.util.StringUtil;

import rossio.ingest.solr.HarvestOaiSourceIntoSolrWithHandler;
import rossio.ingest.solr.RepositoryWithSolr;
import rossio.ingest.solr.manager.TaskThread.Task;
import rossio.ingest.solr.old.HarvestOaiSourceIntoSolr;
import rossio.oaipmh.HarvestException;
import rossio.oaipmh.HarvestReport;
import rossio.util.AccessException;
import rossio.util.HttpsUtil;

public class ManagerOfHarvest implements Task{
	RepositoryWithSolr repository;
	OaiSources oaiSources;
	Logger log;
	int commitInterval;
	String title=this.getClass().getCanonicalName();
	String currentSource="";
	
	Date lastRun=null;
	boolean stopWhenPossible=false;
	
	public ManagerOfHarvest(RepositoryWithSolr repository, OaiSources oaiSources, Logger log, int commitInterval) {
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
		    	List<OaiSource> sourcesToHarvest = oaiSources.getSourcesToHarvest();
		    	if(sourcesToHarvest.isEmpty())
		    		log.log("No sources need harvesting");
				for(OaiSource src: sourcesToHarvest) {
					if(stopWhenPossible) 
						break TOP;
					currentSource=src.getSourceId();
		    		log.log("Start harvesting: "+src.getSourceId());
		    		
		//			HarvestOaiSourceIntoSolr harvest=new HarvestOaiSourceIntoSolr(src.getSourceId(), src.dataProvider, src.baseUrl, src.set, src.metadataPrefix, repository);
					HarvestOaiSourceIntoSolrWithHandler harvest=new HarvestOaiSourceIntoSolrWithHandler(src.getSourceId(), src.dataProvider, src.baseUrl, src.set, src.metadataPrefix, src.lastHarvestTimestamp, repository);
					harvest.setCommitInterval(commitInterval);
					if(!StringUtils.isEmpty(src.resumptionToken)) {
						harvest.resumeWithToken(src.resumptionToken);
						log.log("...resuming with token: "+src.resumptionToken);				
					}else if (src.status!=null && src.status==TaskStatus.CLEAR)
			    		repository.removeAllFrom(src.getSourceId());
		
					Date startOfharvest=new Date();
					HarvestReport report = harvest.run(log);
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
