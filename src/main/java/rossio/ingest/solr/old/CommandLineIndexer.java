package rossio.ingest.solr.old;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map.Entry;
import java.util.logging.LogManager;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.solr.client.solrj.SolrServerException;

import rossio.data.models.Rossio;
import rossio.ingest.solr.Indexer;
import rossio.ingest.solr.IndexingReport;
import rossio.ingest.solr.RepositoryWithSolr;
import rossio.ingest.solr.RepositoryWithSolr.ItemHandler;
import rossio.ingest.solr.manager.Logger;
import rossio.oaipmh.HarvestException;
import rossio.oaipmh.HarvestReport;
import rossio.util.AccessException;
import rossio.util.HttpsUtil;
import rossio.util.RdfUtil;
import rossio.util.RdfUtil.Jena;

public class CommandLineIndexer {

//CommandLineInterface --dataset_uri http://data.bibliotheken.nl/id/dataset/rise-centsprenten --output_file ./data/crawled/rise-centsprenten.nt
	public static void main(String[] args) {
		LogManager.getLogManager().reset();
		HttpsUtil.initSslTrustingHostVerifier();
		CommandLineParser parser = new DefaultParser();
		
		// create the Options
		Options options = new Options();
		options.addOption( "source_id", true, "Identifier of the data source or dataset");
		options.addOption( "solr_url_repository", true, "Solr base URL of the repository core");
		options.addOption( "solr_url_search", true, "Solr base URL of the search core");
		options.addOption( "log_file", true, "Write a log with the result of the harvest. If ommited no log is created.");
		
		CommandLine line=null;

		boolean argsOk=true;
		try {
		    line = parser.parse( options, args );

		    if( !line.hasOption("source_id") || !line.hasOption("solr_url_repository") || !line.hasOption("solr_url_search")  ) 
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
	    if(argsOk) {
	    	logFilePath = line.getOptionValue("log_file");
	    	Logger log= logFilePath==null ? null : new Logger(logFilePath);
	    	RepositoryWithSolr repository=new RepositoryWithSolr(line.getOptionValue("solr_url_repository"));
			Indexer indexer=new Indexer(line.getOptionValue("solr_url_search"));
			String source = line.getOptionValue("source_id");
			
	    	try {
	    		IndexingReport report=indexer.indexSourceFromRepository(source, repository, log);
				result=report.toLogString();
			} catch (Exception e) {
				result="FAILURE\nstackTrace:\n"+ExceptionUtils.getStackTrace(e);
			}
	    } else {
	    	StringWriter sw=new StringWriter();
	    	PrintWriter w=new PrintWriter(sw);
	    	HelpFormatter formatter = new HelpFormatter();
	    	formatter.printUsage( w, 120, "indexer.sh", options );
	    	w.close();
	    	result="INVALID PARAMETERS\n"+sw.toString();
	    }
	    System.out.println(result);
	    if(logFilePath!=null) {
	    	try {
				File logFile = new File(logFilePath);
				if(logFile.getParentFile()!=null && !logFile.getParentFile().exists())
					logFile.getParentFile().mkdirs();
				FileUtils.write(logFile, result, "UTF-8");
			} catch (IOException e) {
				System.out.println("Warning: Unable to write to log file\nStackTrace:\n"+ExceptionUtils.getStackTrace(e));
			}
	    }
	}
}
