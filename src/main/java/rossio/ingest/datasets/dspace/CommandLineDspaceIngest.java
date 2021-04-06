package rossio.ingest.datasets.dspace;

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

import rossio.ingest.dspace.old.HarvestOaiSourceIntoSimpleArchive;
import rossio.ingest.solr.RepositoryWithSolr;
import rossio.oaipmh.HarvestException;
import rossio.oaipmh.HarvestReport;
import rossio.util.AccessException;
import rossio.util.HttpsUtil;

public class CommandLineDspaceIngest {

//CommandLineInterface --dataset_uri http://data.bibliotheken.nl/id/dataset/rise-centsprenten --output_file ./data/crawled/rise-centsprenten.nt
	public static void main(String[] args) {
		LogManager.getLogManager().reset();
		HttpsUtil.initSslTrustingHostVerifier();
		CommandLineParser parser = new DefaultParser();
		
		// create the Options
		Options options = new Options();
//		options.addOption( "source_id", true, "Identifier of the data source or dataset");
		options.addOption( "solr_url_repository", true, "Solr base URL of the repository core");
		options.addOption( "home_folder", true, "Path to the folder where DCAT metadata resides and the output is writen.");
		
		CommandLine line=null;

		boolean argsOk=true;
		try {
		    line = parser.parse( options, args );

		    if( !line.hasOption("solr_url_repository") || !line.hasOption("home_folder")  ) 
		    	argsOk=false;
		    if(argsOk) {
		    	//TODO: check timestamps 
		    }
		} catch( ParseException exp ) {
			argsOk=false;
			exp.printStackTrace();
		}
	    String result=null;
	    if(argsOk) {
	    	File homeFolder = new File(line.getOptionValue("home_folder"));
	    	RepositoryWithSolr repository=new RepositoryWithSolr(line.getOptionValue("solr_url_repository"));
	    	
	    	try {
				DatasetExporterSimpleArchive exporter=new DatasetExporterSimpleArchive(repository);
				exporter.exportAllDatasets(homeFolder);
				result="SUCCESS";
//				HarvestOaiSourceIntoSimpleArchive harvest=new HarvestOaiSourceIntoSimpleArchive(baseUrl, set, metadatPrefix, archive);
//				HarvestReport report = harvest.run();
//				result=report.toLogString();
			} catch (Exception e) {
				result="FAILURE\nstackTrace:\n"+ExceptionUtils.getStackTrace(e);
			}
	    } else {
	    	StringWriter sw=new StringWriter();
	    	PrintWriter w=new PrintWriter(sw);
	    	HelpFormatter formatter = new HelpFormatter();
	    	formatter.printUsage( w, 120, "dstape_ingest_dataset.sh", options );
	    	w.close();
	    	result="INVALID PARAMETERS\n"+sw.toString();
	    }
	    System.out.println(result);
	}
}
