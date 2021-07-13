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
import rossio.ingest.solr.manager.StopFile.StopFileListener;
import rossio.util.Global;
import rossio.util.HttpUtil;
import rossio.util.HttpsUtil;

public class CommandLineManagerOfIndexing {

	//CommandLineInterface --dataset_uri http://data.bibliotheken.nl/id/dataset/rise-centsprenten --output_file ./data/crawled/rise-centsprenten.nt
	public static void main(String[] args) {
		try {
			LogManager.getLogManager().reset();
			HttpsUtil.initSslTrustingHostVerifier();
			CommandLineParser parser = new DefaultParser();
			
			// create the Options
			Options options = new Options();
			options.addOption( "sources_file", true, "A file listing the OAI-PMH sources");
			options.addOption( "indexing_status_file", true, "A file listing the indexing status of the OAI-PMH sources");
			options.addOption( "solr_url_repository", true, "Solr base URL of the repository core");
			options.addOption( "solr_url_search", true, "Solr base URL of the search core");
			options.addOption( "sparql_vocabs", true, "Sparql endpoint URL with the ROSSIO vocabularies");
			options.addOption( "debug", false, "Print debuging information");
			options.addOption( "commit_interval", true, "Commit changes every x records");
			options.addOption( "log_file", true, "Write a log with the result of the indexing. If ommited no log is created.");
			
			CommandLine line=null;
	
			boolean argsOk=true;
			try {
			    line = parser.parse( options, args );
	
			    if( !line.hasOption("sources_file") || !line.hasOption("solr_url_repository") ||
			    		!line.hasOption("solr_url_search") || !line.hasOption("indexing_status_file")
			    		 || !line.hasOption("sparql_vocabs")) 
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
		    	String sparqlVocabs=line.getOptionValue("sparql_vocabs");
		    	File sourcesFile = new File(line.getOptionValue("sources_file"));
		    	File indexingStatusFile = new File(line.getOptionValue("indexing_status_file"));
		    	File indexingStatusLockFile = new File(line.getOptionValue("indexing_status_file")+".lock");
		    	File stopFileFile = new File(line.getOptionValue("indexing_status_file")+".stop");
				StopFile stopFile = new StopFile(stopFileFile);
		    	if(indexingStatusLockFile.exists()) {
			    	System.out.println("Lock file found at "+indexingStatusLockFile.getCanonicalPath()+
			    			" (Is another instance of this program running? If not, remove the lock file before executing.)");
			    	System.out.println("Exiting without running...");
		    		return;
		    	}else {
		    		FileUtils.write(indexingStatusLockFile, "", StandardCharsets.UTF_8);
					indexingStatusLockFile.deleteOnExit();
		    	}
		    	
				OaiSources oaiSources=new OaiSources(sourcesFile);
				OaiSourcesIndexStatus indexStatus=new OaiSourcesIndexStatus(indexingStatusFile, oaiSources);

				Global.init_componentHttpRequestService();
				
		    	Indexer indexer=new Indexer(line.getOptionValue("solr_url_search"));
		    	indexer.setCommitInterval(commitInterval);
		    	TaskThread indexerTask=null;
		    	ManagerOfIndexing managerIndexer=new ManagerOfIndexing(repository, indexer, indexStatus, sparqlVocabs, log, commitInterval);
		    	indexerTask=new TaskThread(managerIndexer, log);

		    	stopFile.addListener(indexerTask);
		    	stopFile.addListener(new StopFileListener() {
					@Override
					public void signalStop() {
						try {
							FileUtils.write(stopFileFile, "", StandardCharsets.UTF_8);
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				});
		    	stopFile.waitUntilStop();    			
		    	
			    	if (indexerTask.getError()!=null) {
			    		System.out.println("Indexer exited with ERROR: ");
			    		indexerTask.getError().printStackTrace(System.out);
			    	} else
			    		System.out.println("Indexer exited with SUCCESS");
			    	
				log.log(oaiSources.printStatus());
		    } else {
		    	StringWriter sw=new StringWriter();
		    	PrintWriter w=new PrintWriter(sw);
		    	HelpFormatter formatter = new HelpFormatter();
		    	formatter.printUsage( w, 120, "indexing_manager.sh", options );
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
