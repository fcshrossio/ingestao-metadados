package rossio.export;

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
import rossio.ingest.solr.manager.Logger;
import rossio.ingest.solr.manager.MetadataSources;
import rossio.ingest.solr.manager.StopFile.StopFileListener;
import rossio.util.Global;
import rossio.util.HttpUtil;
import rossio.util.HttpsUtil;

public class CommandLineDatasetExporter {

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
			options.addOption( "export_folder", true, "The folder where the datasets will be exported to");
			options.addOption( "source_id", true, "Source ID of the collection to export");
			options.addOption( "sample_size", true, "Export only a sample of this size per collection");
			
			CommandLine line=null;
	
			boolean argsOk=true;
			try {
			    line = parser.parse( options, args );
	
			    if( !line.hasOption("sources_file") || !line.hasOption("solr_url_repository") 
			    		|| !line.hasOption("export_folder") )	 
			    	argsOk=false;
			} catch( ParseException exp ) {
				argsOk=false;
				exp.printStackTrace();
			}
		    String result=null;
		    if(argsOk) {
		    	Global.DEBUG=line.hasOption("debug");
		    	
		    	RepositoryWithSolr repository=new RepositoryWithSolr(line.getOptionValue("solr_url_repository"));
		    	File sourcesFile = new File(line.getOptionValue("sources_file"));
		    	File exportFolder = new File(line.getOptionValue("export_folder"));
		    	
		    	int sampleSize=-1;
		    	if(!StringUtils.isEmpty(line.getOptionValue("sample_size")))
		    		sampleSize=Integer.parseInt(line.getOptionValue("sample_size"));
		    	String sourceId=null;
		    	if(!StringUtils.isEmpty(line.getOptionValue("source_id")))
		    		sourceId=line.getOptionValue("source_id");
		    	
				MetadataSources oaiSources=new MetadataSources(sourcesFile);
				DatasetExporter exporter=new DatasetExporter(repository, oaiSources);
				
				if(sourceId==null)
					exporter.exportAllDatasets(exportFolder, sampleSize);
				else
					exporter.exportDataset(exportFolder, sourceId, sampleSize);
					
		    } else {
		    	StringWriter sw=new StringWriter();
		    	PrintWriter w=new PrintWriter(sw);
		    	HelpFormatter formatter = new HelpFormatter();
		    	formatter.printUsage( w, 120, "dataset_export.sh", options );
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
