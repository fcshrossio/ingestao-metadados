package rossio.ingest.solr.manager;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.FileWriterWithEncoding;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ResIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RiotException;

import rossio.data.models.Dcat;
import rossio.data.models.Rdf;
import rossio.data.models.Rdfs;
import rossio.data.models.Rossio;
import rossio.util.RdfUtil;
import rossio.util.RdfUtil.Jena;


public class MetadataSources{
	File sourcesFile;
	List<MetadataSource> sources;

	protected MetadataSources() {
	}
	
	public MetadataSources(File sourcesFile) throws IOException {
		super();
		this.sourcesFile = sourcesFile;

		refresh();
	}
	
	public void refresh() throws IOException {
		sources=new ArrayList<MetadataSource>();
		Model m;
		try {
			m = RdfUtil.readRdf(sourcesFile, Lang.TURTLE);
		} catch (Exception e) { //retry. other process could be writing
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e1) {	}
			m = RdfUtil.readRdf(sourcesFile, Lang.TURTLE);
		}
		for(Resource dsRes: m.listResourcesWithProperty(Rdf.type, Dcat.Dataset).toList()) {
			sources.add(new MetadataSource(dsRes));
		}		
	}
	
	public static MetadataSources readFromTxt(File sourcesFileTxt) throws IOException {
		MetadataSources srcs=new MetadataSources();
		
		List<String> sourcesUnparsed = FileUtils.readLines(sourcesFileTxt, StandardCharsets.UTF_8);
		srcs.sources=new ArrayList<MetadataSource>(sourcesUnparsed.size());
		for(String unparsed:sourcesUnparsed) {
			if(!StringUtils.isEmpty(unparsed) && ! unparsed.startsWith("#")) {
				MetadataSource src = new MetadataSource(unparsed);
				if(src.status==TaskStatus.SUCCESS)
					src.lastHarvestTimestamp=new Date();
				srcs.sources.add(src);
			}
		}
		return srcs;
	}
	
	public synchronized List<MetadataSource> getAllSources() {
		return sources;
	}
	

	public synchronized List<MetadataSource> getSourcesToHarvest() throws IOException {
		List<MetadataSource> srcs=new ArrayList<MetadataSource>(getAllSources());
		for(Iterator<MetadataSource> it=srcs.iterator(); it.hasNext() ; ) {
			MetadataSource s=it.next();
			if (s.status!=null) {
				if(s.status== TaskStatus.PAUSED || !s.todoHarvest() ) 
					it.remove();				
			}
		}
		return srcs;
	}

//	public synchronized List<OaiSource> getSourcesToIndex() throws IOException {
//		List<OaiSource> srcs=new ArrayList<OaiSource>(getAllSources());
//		for(Iterator<OaiSource> it=srcs.iterator(); it.hasNext() ; ) {
//			OaiSource s=it.next();
//			if (StringUtils.isEmpty(s.status) || !s.status.equals("SUCCESS"))
//				it.remove();
//			else if (!StringUtils.isEmpty(s.statusIndexing) && (s.statusIndexing.equals("SUCCESS") || s.statusIndexing.equals("PAUSED")))
//				it.remove();
//		}
//		return srcs;
//	}


	public synchronized String printStatus() throws IOException {
		return String.format("Sources: %d ; To harvest: %d ; FAILURES(harvest): %d ", 
				getAllSources().size(), getSourcesToHarvest().size(),
				getSourcesFailToHarvest().size()
				);
//		return String.format("Sources: %d ; To harvest: %d ; To index: %d ; FAILURES(harvest): %d ; FAILURES(index): %d", 
//				getAllSources().size(), getSourcesToHarvest().size(), getSourcesToIndex().size(),
//				getSourcesFailToHarvest().size()
//				,getSourcesFailToIndex().size()
//				);
	}
	
	public synchronized List<MetadataSource> getSourcesFailToHarvest() throws IOException {
		List<MetadataSource> srcs=new ArrayList<MetadataSource>(getAllSources());
		for(Iterator<MetadataSource> it=srcs.iterator(); it.hasNext() ; ) {
			MetadataSource s=it.next();
			if (s.status==null || s.status!=TaskStatus.FAILURE)
				it.remove();
		}
		return srcs;
	}
//	public synchronized List<OaiSource> getSourcesFailToIndex() throws IOException {
//		List<OaiSource> srcs=new ArrayList<OaiSource>(getAllSources());
//		for(Iterator<OaiSource> it=srcs.iterator(); it.hasNext() ; ) {
//			OaiSource s=it.next();
//			if (StringUtils.isEmpty(s.statusIndexing) || !s.statusIndexing.equals("FAILURE"))
//				it.remove();
//		}
//		return srcs;
//	}
	public synchronized void save() throws IOException {
//		Writer writer=new FileWriterWithEncoding(sourcesFile, StandardCharsets.UTF_8);
//		for(OaiSource src: sources) {
//			writer.write(src.serializeToString());
//			writer.write("\n");
//		}
//		writer.close();
		
		Model m=Jena.createModel();
		for(MetadataSource source:getAllSources()) {
			if(source.uri==null)
				source.uri=Rossio.NS_CONJUNTO_DE_DADOS + UUID.randomUUID().toString();
			Resource r=source.toRdf(m);
		}
		FileOutputStream out = new FileOutputStream(sourcesFile);
		m.setNsPrefix("", Rossio.NS_CONJUNTO_DE_DADOS);
		m.setNsPrefix("rossio", Rossio.NS+"ingestao/");
		m.setNsPrefix("dcat", Dcat.NS);
		m.setNsPrefix("rdf", Rdf.NS);
		m.setNsPrefix("rdfs", Rdfs.NS);
		RdfUtil.writeRdf(m, Lang.TURTLE, out);
		out.close();
	}

	public void setSourcesFile(File sourcesFile) {
		this.sourcesFile = sourcesFile;
	}

	public MetadataSource findSource(String uri) {
		for(MetadataSource source:getAllSources()) {
			if(source.uri.equals(uri))
				return source;
		}
		return null;
	}
}