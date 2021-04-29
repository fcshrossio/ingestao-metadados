package rossio.ingest.solr.manager;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.FileWriterWithEncoding;
import org.apache.commons.lang3.StringUtils;


public class OaiSources{
	File sourcesFile;
	List<OaiSource> sources;

	public OaiSources(File sourcesFile) {
		super();
		this.sourcesFile = sourcesFile;
	}
	
	public synchronized List<OaiSource> getAllSources() throws IOException {
		if(sources==null) {
			List<String> sourcesUnparsed = FileUtils.readLines(sourcesFile, StandardCharsets.UTF_8);
			sources=new ArrayList<OaiSource>(sourcesUnparsed.size());
			for(String unparsed:sourcesUnparsed) {
				if(!StringUtils.isEmpty(unparsed))
					sources.add(new OaiSource(unparsed));
			}
		}
		return sources;
	}
	

	public synchronized List<OaiSource> getSourcesToHarvest() throws IOException {
		List<OaiSource> srcs=new ArrayList<OaiSource>(getAllSources());
		for(Iterator<OaiSource> it=srcs.iterator(); it.hasNext() ; ) {
			OaiSource s=it.next();
			if (!StringUtils.isEmpty(s.status) && (s.status.equals("SUCCESS") || s.status.equals("PAUSED")))
				it.remove();
		}
		return srcs;
	}

	public synchronized List<OaiSource> getSourcesToIndex() throws IOException {
		List<OaiSource> srcs=new ArrayList<OaiSource>(getAllSources());
		for(Iterator<OaiSource> it=srcs.iterator(); it.hasNext() ; ) {
			OaiSource s=it.next();
			if (StringUtils.isEmpty(s.status) || !s.status.equals("SUCCESS"))
				it.remove();
			else if (!StringUtils.isEmpty(s.statusIndexing) && (s.statusIndexing.equals("SUCCESS") || s.statusIndexing.equals("PAUSED")))
				it.remove();
		}
		return srcs;
	}


	public synchronized String printStatus() throws IOException {
		return String.format("Sources: %d ; To harvest: %d ; To index: %d ; FAILURES(harvest): %d ; FAILURES(index): %d", 
				getAllSources().size(), getSourcesToHarvest().size(), getSourcesToIndex().size(),
				getSourcesFailToHarvest().size(),
				getSourcesFailToIndex().size()
				);
	}
	
	public synchronized List<OaiSource> getSourcesFailToHarvest() throws IOException {
		List<OaiSource> srcs=new ArrayList<OaiSource>(getAllSources());
		for(Iterator<OaiSource> it=srcs.iterator(); it.hasNext() ; ) {
			OaiSource s=it.next();
			if (StringUtils.isEmpty(s.status) || !s.status.equals("FAILURE"))
				it.remove();
		}
		return srcs;
	}
	public synchronized List<OaiSource> getSourcesFailToIndex() throws IOException {
		List<OaiSource> srcs=new ArrayList<OaiSource>(getAllSources());
		for(Iterator<OaiSource> it=srcs.iterator(); it.hasNext() ; ) {
			OaiSource s=it.next();
			if (StringUtils.isEmpty(s.statusIndexing) || !s.statusIndexing.equals("FAILURE"))
				it.remove();
		}
		return srcs;
	}
	public synchronized void save() throws IOException {
		Writer writer=new FileWriterWithEncoding(sourcesFile, StandardCharsets.UTF_8);
		for(OaiSource src: sources) {
			writer.write(src.serializeToString());
			writer.write("\n");
		}
		writer.close();
	}
}