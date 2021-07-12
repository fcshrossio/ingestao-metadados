package rossio.ingest.solr;

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

import rossio.ingest.solr.manager.Logger;
import rossio.ingest.solr.manager.OaiSource;
import rossio.ingest.solr.manager.OaiSources;
import rossio.ingest.solr.manager.StopFile;
import rossio.oaipmh.HarvestException;
import rossio.oaipmh.HarvestReport;
import rossio.util.AccessException;
import rossio.util.HttpsUtil;

public class CommandLineRemoveCollectionFromRepository {
	
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
			options.addOption( "source_id", true, "Source ID of the collection");
			
			CommandLine line=null;
	
			boolean argsOk=true;
			try {
			    line = parser.parse( options, args );
	
			    if( !line.hasOption("source_id") || !line.hasOption("solr_url_repository")  ) 
			    	argsOk=false;
			} catch( ParseException exp ) {
				argsOk=false;
				exp.printStackTrace();
			}
		    String result=null;
		    if(argsOk) {
		    	RepositoryWithSolr repo=new RepositoryWithSolr(line.getOptionValue("solr_url_repository"));
		    	File sourcesFile = new File(line.getOptionValue("sources_file"));
				OaiSources oaiSources=new OaiSources(sourcesFile);
		    	OaiSource src = oaiSources.findSource(line.getOptionValue("source_id"));
		    	if(src==null)
		    		repo.removeAllFrom(line.getOptionValue("source_id"));
		    	else
		    		repo.removeAllFrom(src.getSourceIdDeprecated());
				result="SUCCESS";
		    } else {
		    	StringWriter sw=new StringWriter();
		    	PrintWriter w=new PrintWriter(sw);
		    	HelpFormatter formatter = new HelpFormatter();
		    	formatter.printUsage( w, 120, "manager-of-harvest.sh", options );
		    	w.close();
		    	result="INVALID PARAMETERS\n"+sw.toString();
		    }
		    System.out.println(result);
		} catch (Exception e) {
			System.err.println("FATAL EXCEPTION:");
			e.printStackTrace();
		}
	}
	
}
