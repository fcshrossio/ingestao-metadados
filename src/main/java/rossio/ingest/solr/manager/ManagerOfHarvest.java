package rossio.ingest.solr.manager;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
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
	
	public ManagerOfHarvest(RepositoryWithSolr repository, OaiSources oaiSources, Logger log, int commitInterval) {
		super();
		this.repository = repository;
		this.oaiSources = oaiSources;
		this.log = log;
		this.commitInterval = commitInterval;
	}

	public void run() throws Exception {
    	List<OaiSource> sourcesToHarvest = oaiSources.getSourcesToHarvest();
		for(OaiSource src: sourcesToHarvest) {
			currentSource=src.getSourceId();
    		log.log("Start harvesting: "+src.getSourceId());
    		
//			HarvestOaiSourceIntoSolr harvest=new HarvestOaiSourceIntoSolr(src.getSourceId(), src.dataProvider, src.baseUrl, src.set, src.metadataPrefix, repository);
			HarvestOaiSourceIntoSolrWithHandler harvest=new HarvestOaiSourceIntoSolrWithHandler(src.getSourceId(), src.dataProvider, src.baseUrl, src.set, src.metadataPrefix, repository);
			harvest.setCommitInterval(commitInterval);
			if(src.resumptionToken!=null) {
				harvest.resumeWithToken(src.resumptionToken);
				log.log("...resuming with token: "+src.resumptionToken);				
			}else if (src.status.equals("CLEAR"))
	    		repository.removeAllFrom(src.getSourceId());

			HarvestReport report = harvest.run(log);
			String result=report.toLogString();

			if(!report.isSuccessful() && report.getResumptionTokenOfLastCommit()!=null)
				src.resumptionToken=report.getResumptionTokenOfLastCommit();
			else
				src.resumptionToken=null;
			if(report.isSuccessful())
				src.statusIndexing="OUTDATED";
				
			log.log("End: "+src.getSourceId());
	    	log.log(result);
	    	src.updateStatus(result);
	    	oaiSources.save();
	    	System.out.println("Harvest concluded: "+src.getSourceId());
    	}
	}

	@Override
	public String getTitle() {
		return title+" - "+currentSource;
	}
}
