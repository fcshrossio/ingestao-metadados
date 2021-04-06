package rossio.ingest.solr.old;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
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
import org.apache.commons.io.output.FileWriterWithEncoding;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.xerial.snappy.OSInfo;

import com.ctc.wstx.util.StringUtil;

import rossio.ingest.solr.RepositoryWithSolr;
import rossio.ingest.solr.manager.Logger;
import rossio.ingest.solr.manager.OaiSource;
import rossio.ingest.solr.manager.OaiSources;
import rossio.oaipmh.HarvestException;
import rossio.oaipmh.HarvestReport;
import rossio.util.AccessException;
import rossio.util.HttpsUtil;

public class CommandLineManagerOfHarvest {

//	
//	
//	
////CommandLineInterface --dataset_uri http://data.bibliotheken.nl/id/dataset/rise-centsprenten --output_file ./data/crawled/rise-centsprenten.nt
//	public static void main(String[] args) {
//		try {
//			LogManager.getLogManager().reset();
//			HttpsUtil.initSslTrustingHostVerifier();
//			CommandLineParser parser = new DefaultParser();
//			
//			// create the Options
//			Options options = new Options();
//			options.addOption( "sources_file", true, "A file listing the OAI-PMH sources");
//			options.addOption( "solr_url_repository", true, "Solr base URL of the repository core");
//			options.addOption( "commit_interval", true, "Commit changes every x records");
//			options.addOption( "log_file", true, "Write a log with the result of the harvests. If ommited no log is created.");
//			
//			CommandLine line=null;
//	
//			boolean argsOk=true;
//			try {
//			    line = parser.parse( options, args );
//	
//			    if( !line.hasOption("sources_file") || !line.hasOption("solr_url_repository")  ) 
//			    	argsOk=false;
//			    if(argsOk) {
//			    	//TODO: check timestamps 
//			    }
//			} catch( ParseException exp ) {
//				argsOk=false;
//				exp.printStackTrace();
//			}
//		    String result=null;
//		    String logFilePath=null;
//		    int commitInterval=20000;
//		    Logger log=null;
//		    OaiSources oaiSources=null;
//		    if(argsOk) {
//		    	logFilePath = line.getOptionValue("log_file");
//		    	if(!StringUtils.isEmpty(logFilePath))
//		    		log=new Logger(logFilePath);
//		    	else
//		    		log=new Logger(null);
//		    	if(!StringUtils.isEmpty(line.getOptionValue("commit_interval")))
//	    			commitInterval = Integer.parseInt(line.getOptionValue("commit_interval"));
//		    	
//		    	RepositoryWithSolr repository=new RepositoryWithSolr(line.getOptionValue("solr_url_repository"));
//		    	oaiSources=new OaiSources(new File(line.getOptionValue("sources_file")));
//				
//		    	List<OaiSource> sourcesToHarvest = oaiSources.getSourcesToHarvest();
//				for(OaiSource src: sourcesToHarvest) {
//		    		log.log("Start: "+src.getSourceId());
//		    		repository.removeAllFrom(src.getSourceId());
//		    		
//					HarvestOaiSourceIntoSolr harvest=new HarvestOaiSourceIntoSolr(src.getSourceId(), src.baseUrl, src.set, src.metadataPrefix, repository);
//					harvest.setCommitInterval(commitInterval);
//						HarvestReport report = harvest.run(log);
////					HarvestReport report = harvest.run(10);
//					result=report.toLogString();
//
//					log.log("End: "+src.getSourceId());
//			    	log.log(result);
//			    	src.updateSatus(result);
//			    	oaiSources.save();
//			    	System.out.println("Harvest concluded: "+src.getSourceId());
//		    	}
//				result="SUCCESS";
//
//				log.log(oaiSources.printStatus());
//				log.log(result);
//				System.out.println(result);
//		    } else {
//		    	StringWriter sw=new StringWriter();
//		    	PrintWriter w=new PrintWriter(sw);
//		    	HelpFormatter formatter = new HelpFormatter();
//		    	formatter.printUsage( w, 120, "manager-of-harvest.sh", options );
//		    	w.close();
//		    	result="INVALID PARAMETERS\n"+sw.toString();
//		    	log.log(result);
//		    }
//		} catch (Exception e) {
//			System.err.println("FATAL EXCEPTION:");
//			e.printStackTrace();
//		}
//	}
	
}
