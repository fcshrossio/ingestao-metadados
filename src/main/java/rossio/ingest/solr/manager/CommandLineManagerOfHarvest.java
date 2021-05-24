package rossio.ingest.solr.manager;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
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


import rossio.ingest.solr.Indexer;
import rossio.ingest.solr.RepositoryWithSolr;
import rossio.util.Global;
import rossio.util.HttpUtil;
import rossio.util.HttpsUtil;

public class CommandLineManagerOfHarvest {

	//CommandLineInterface --dataset_uri http://data.bibliotheken.nl/id/dataset/rise-centsprenten --output_file ./data/crawled/rise-centsprenten.nt
	public static void main(String[] args) {
		try {
			LogManager.getLogManager().reset();
			HttpsUtil.initSslTrustingHostVerifier();
			CommandLineParser parser = new DefaultParser();
			
			// create the Options
			Options options = new Options();
			options.addOption( "sources_file", true, "A file listing the OAI-PMH sources");
			options.addOption( "solr_url_repository", true, "Solr base URL of the repository core");
			options.addOption( "debug", false, "Print debuging information");
			options.addOption( "commit_interval", true, "Commit changes every x records");
			options.addOption( "log_file", true, "Write a log with the result of the harvests. If ommited no log is created.");
			
			CommandLine line=null;
	
			boolean argsOk=true;
			try {
			    line = parser.parse( options, args );
	
			    if( !line.hasOption("sources_file") || !line.hasOption("solr_url_repository") )	 
			    	argsOk=false;
			    if(argsOk) {
			    	//TODO: check timestamps 
			    }
			} catch( ParseException exp ) {
				argsOk=false;
				exp.printStackTrace();
			}
		    String result=null;
		    String logFilePath=null;
		    int commitInterval=20000;
//		    int commitInterval=1000;
		    Logger log=null;
		    if(argsOk) {
		    	logFilePath = line.getOptionValue("log_file");
		    	Global.DEBUG=line.hasOption("debug");
		    	if(!StringUtils.isEmpty(logFilePath))
		    		log=new Logger(logFilePath);
		    	else
		    		log=new Logger(null);
		    	if(!StringUtils.isEmpty(line.getOptionValue("commit_interval")))
	    			commitInterval = Integer.parseInt(line.getOptionValue("commit_interval"));
		    	
		    	RepositoryWithSolr repository=new RepositoryWithSolr(line.getOptionValue("solr_url_repository"));
		    	File sourcesFile = new File(line.getOptionValue("sources_file"));
		    	File sourcesLockFile = new File(line.getOptionValue("sources_file")+".lock");
		    	StopFile stopFile = new StopFile(new File(line.getOptionValue("sources_file")+".stop"));
		    	if(sourcesLockFile.exists()) {
			    	System.out.println("Lock file found at "+sourcesLockFile.getCanonicalPath()+
			    			" (Is another instance of this program running? If not, remove the file before executing.)");
			    	System.out.println("Exiting without running...");
		    		return;
		    	} else {
		    		FileUtils.write(sourcesLockFile, "", StandardCharsets.UTF_8);
					sourcesLockFile.deleteOnExit();
		    	}
		    	
				OaiSources oaiSources=new OaiSources(sourcesFile);

				Global.init_componentHttpRequestService();
				
		    	ManagerOfHarvest managerHarvester=new ManagerOfHarvest(repository, oaiSources, log, commitInterval);
		    	TaskThread harvesterTask=new TaskThread(managerHarvester, log);

		    	stopFile.addListener(harvesterTask);
		    	stopFile.waitUntilStop();
	    		
		    	if (harvesterTask.getError()!=null) {
		    		System.out.println("Harvester exited with ERROR: ");
		    		harvesterTask.getError().printStackTrace(System.out);
		    	} else
		    		System.out.println("Harvester exited with SUCCESS");
		    	
				log.log(oaiSources.printStatus());
		    } else {
		    	StringWriter sw=new StringWriter();
		    	PrintWriter w=new PrintWriter(sw);
		    	HelpFormatter formatter = new HelpFormatter();
		    	formatter.printUsage( w, 120, "harvest_manager.sh", options );
		    	w.close();
		    	result="INVALID PARAMETERS\n"+sw.toString();
		    	System.out.println(result);
		    }
		} catch (Exception e) {
			System.err.println("FATAL EXCEPTION:");
			e.printStackTrace();
		}
	}
	
}
