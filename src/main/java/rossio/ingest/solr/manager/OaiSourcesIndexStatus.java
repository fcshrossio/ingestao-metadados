package rossio.ingest.solr.manager;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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


public class OaiSourcesIndexStatus{
	File sourcesFile;
	List<OaiSourceIndexStatus> sources;
	MetadataSources oaiSources;

	protected OaiSourcesIndexStatus() {
	}
	
	public OaiSourcesIndexStatus(File sourcesFile, MetadataSources oaiSources) throws IOException {
		super();
		this.sourcesFile = sourcesFile;
		this.oaiSources = oaiSources;

		refresh();
	}
	
	
	public synchronized List<OaiSourceIndexStatus> getAllSources() throws IOException {
		return sources;
	}
	

	public synchronized List<OaiSourceIndexStatus> getSourcesToIndex() throws IOException {
		List<OaiSourceIndexStatus> srcs=new ArrayList<OaiSourceIndexStatus>(getAllSources());
		for(Iterator<OaiSourceIndexStatus> it=srcs.iterator(); it.hasNext() ; ) {
			OaiSourceIndexStatus s=it.next();
			if (!s.todoIndex() ) 
				it.remove();				
		}
		return srcs;
	}

	public synchronized String printStatus() throws IOException {
		return String.format("Sources: %d ; To index: %d ; FAILURES(index): %d", 
				getAllSources().size(),getSourcesToIndex().size(),
				getSourcesFailToIndex().size()
				);
//		return String.format("Sources: %d ; To index: %d ; FAILURES(index): %d", 
//				getAllSources().size(),getSourcesToIndex().size(),
//				getSourcesFailToIndex().size()
//				);
	}
	
	public synchronized List<OaiSourceIndexStatus> getSourcesFailToIndex() throws IOException {
		List<OaiSourceIndexStatus> srcs=new ArrayList<OaiSourceIndexStatus>(getAllSources());
		for(Iterator<OaiSourceIndexStatus> it=srcs.iterator(); it.hasNext() ; ) {
			OaiSourceIndexStatus s=it.next();
			if (s.status==null  || s.status!=TaskStatus.FAILURE)
				it.remove();
		}
		return srcs;
	}
	public synchronized void save() throws IOException {
//		Writer writer=new FileWriterWithEncoding(sourcesFile, StandardCharsets.UTF_8);
//		for(OaiSource src: sources) {
//			writer.write(src.serializeToString());
//			writer.write("\n");
//		}
//		writer.close();
		
		Model m=Jena.createModel();
		for(OaiSourceIndexStatus source:getAllSources()) {
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

	public void refresh() throws IOException {
		sources=new ArrayList<OaiSourceIndexStatus>();
		Model m;
		if(sourcesFile.exists())
			m=RdfUtil.readRdf(sourcesFile, Lang.TURTLE);
		else
			m=Jena.createModel();
		for(Resource dsRes: m.listResourcesWithProperty(Rdf.type, Dcat.Dataset).toList()) {
			MetadataSource oaiSource = oaiSources.findSource(dsRes.getURI());
			if(oaiSource!=null)
				sources.add(new OaiSourceIndexStatus(dsRes, oaiSource));
		}
		
		for(MetadataSource src: oaiSources.sources) {
			if(!RdfUtil.exists(src.uri , m)) {
				Resource dsRes=m.createResource(src.uri, Dcat.Dataset);
				sources.add(new OaiSourceIndexStatus(dsRes, src));
			}
		}	
	}
}