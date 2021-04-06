package rossio.ingest.dspace.old;

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

import rossio.ingest.datasets.dspace.SimpleArchive;
import rossio.oaipmh.HarvestException;
import rossio.oaipmh.HarvestReport;
import rossio.util.AccessException;
import rossio.util.HttpsUtil;

public class CommandLineInterface {

//CommandLineInterface --dataset_uri http://data.bibliotheken.nl/id/dataset/rise-centsprenten --output_file ./data/crawled/rise-centsprenten.nt
	public static void main(String[] args) {
		LogManager.getLogManager().reset();
		HttpsUtil.initSslTrustingHostVerifier();
		CommandLineParser parser = new DefaultParser();
		
		// create the Options
		Options options = new Options();
		options.addOption( "base_url", true, "Base URL of the OAI-PMH server");
		options.addOption( "set", true, "OAI-PMH set");
		options.addOption( "metadata_prefix", true, "OAI-PMH metadata prefix");
		options.addOption( "from", true, "From timestamp");
		options.addOption( "until", true, "Until timestamp");
		options.addOption( "output_file", true, "Path to the output file.");
		options.addOption( "log_file", true, "Write a log with the result of the harvest. If ommited no log is created.");
		
		CommandLine line=null;

		boolean argsOk=true;
		try {
		    line = parser.parse( options, args );

		    if( !line.hasOption("base_url") || !line.hasOption("metadata_prefix")  ) 
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
	    	String baseUrl = line.getOptionValue("base_url");
	    	String set = line.getOptionValue("set");
	    	String metadatPrefix = line.getOptionValue("metadata_prefix");
//	    	boolean datasetDescriptionOnly = line.hasOption("dataset_description_only");
	    	
	    	try {
		    	File simpleArchiveZip=new File(line.getOptionValue("output_file"));
				SimpleArchive archive=new SimpleArchive(simpleArchiveZip);
	    	
				HarvestOaiSourceIntoSimpleArchive harvest=new HarvestOaiSourceIntoSimpleArchive(baseUrl, set, metadatPrefix, archive);
				HarvestReport report = harvest.run();
				result=report.toLogString();
			} catch (HarvestException | IOException e) {
				result="FAILURE\nstackTrace:\n"+ExceptionUtils.getStackTrace(e);
			}
	    } else {
	    	StringWriter sw=new StringWriter();
	    	PrintWriter w=new PrintWriter(sw);
	    	HelpFormatter formatter = new HelpFormatter();
	    	formatter.printUsage( w, 120, "crawler.sh", options );
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
