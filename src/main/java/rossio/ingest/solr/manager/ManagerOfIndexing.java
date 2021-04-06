package rossio.ingest.solr.manager;

import java.util.List;
import org.apache.commons.lang3.exception.ExceptionUtils;
import rossio.ingest.solr.Indexer;
import rossio.ingest.solr.IndexingReport;
import rossio.ingest.solr.RepositoryWithSolr;
import rossio.ingest.solr.manager.TaskThread.Task;

public class ManagerOfIndexing  implements Task {

	RepositoryWithSolr repository;
	Indexer indexer;
	OaiSources oaiSources;
	Logger log;
	int commitInterval;
	String title=this.getClass().getCanonicalName();
	String currentSource="";
	
	public ManagerOfIndexing(RepositoryWithSolr repository, Indexer indexer,OaiSources oaiSources, Logger log, int commitInterval) {
		super();
		this.repository = repository;
		this.indexer = indexer;
		this.oaiSources = oaiSources;
		this.log = log;
		this.commitInterval = commitInterval;
	}

	public void run() throws Exception {
    	List<OaiSource> sourcesToIndex = oaiSources.getSourcesToIndex();
    	log.log("Will index "+sourcesToIndex.size()+" sources");
		for(OaiSource src: sourcesToIndex) {
			currentSource=src.getSourceId();
    		log.log("Start indexing: "+src.getSourceId());

    		String result=null;
	    	try {
	    		IndexingReport report=indexer.indexSourceFromRepository(src.getSourceId(), repository, log);
				result=report.toLogString();
			} catch (Exception e) {
				result="FAILURE\nstackTrace:\n"+ExceptionUtils.getStackTrace(e);
			}

			log.log("End indexing: "+src.getSourceId());
	    	log.log(result);
	    	src.updateStatusIndexing(result);
	    	oaiSources.save();
	    	System.out.println("Indexing concluded: "+src.getSourceId());
    	}
	}
	@Override
	public String getTitle() {
		return title+" - "+currentSource;
	}
}
