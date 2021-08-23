package rossio.ingest.solr;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.NodeIterator;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Seq;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.RDFWriter;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient.RemoteSolrException;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import rossio.data.models.DcTerms;
import rossio.data.models.Edm;
import rossio.data.models.Rossio;
import rossio.enrich.metadata.EnrichmentTask;
import rossio.ingest.solr.RepositoryWithSolr.ItemHandler;
import rossio.ingest.solr.manager.Logger;
import rossio.util.RdfUtil;
import rossio.util.RdfUtil.Jena;

public class Indexer {
	SolrClient solr;
	boolean runEnrichment=true;
	int commitInterval=20000;

	public Indexer(String solrUrl) {
//		final String solrUrl = "http://localhost:8983/solr";
//		solr= new HttpSolrClient.Builder(solrUrl)
//		    .withConnectionTimeout(10000)
//		    .withSocketTimeout(60000)
//		    .build();
		solr= new ConcurrentUpdateSolrClient.Builder(solrUrl)
				.withConnectionTimeout(10000)
				.withSocketTimeout(60000)
				.build();
	}
	

	public void restart() throws SolrServerException, IOException {
		solr.rollback();
	}

	public void abort() throws SolrServerException, IOException {
		solr.rollback();
	}

	public void end() throws SolrServerException, IOException {
		solr.commit();
	}

//	public void addItem(String source, Resource item) throws SolrServerException, IOException {
	public void addItem(String source, Model model, String providedChoUri) throws SolrServerException, IOException {
		Resource cho = model.createResource(providedChoUri);
		Resource aggregation = model.createResource(providedChoUri+"#aggregation");
		Resource proxy = RdfUtil.getResourceIfExists(cho.getURI()+"#proxy", model);
		
		String itemId = providedChoUri.substring(providedChoUri.lastIndexOf('/'));
		final SolrInputDocument doc = new SolrInputDocument();
		doc.addField("id", itemId);
		doc.addField("rossio_source", source);
		doc.addField("rossio_provider", aggregation.getProperty(Edm.dataProvider).getObject().asResource().getURI());
		
		JSONObject agg = new JSONObject();
//		writePropertiesToJson(aggregation, agg);
//		agg.put("edm_dataProvider", aggregation.getProperty(Edm.dataProvider).getObject().asResource().getURI());
//		agg.put("edm_datasetName", aggregation.getProperty(Edm.datasetName).getObject().asLiteral().getString());
		writePropertiesToJsonAndSolrDoc(aggregation, agg, null);
		
		JSONObject proxyJson = null;
		if(proxy!=null) {
			proxyJson = new JSONObject();
			writePropertiesToJsonAndSolrDoc(proxy, proxyJson, null);

			//add normalized dates to a particular solr field for date ranges
			for(Statement st:proxy.listProperties(DcTerms.date).toList()) {
				if (st.getObject().isLiteral()) 
					doc.addField("dcterms_date_range", st.getObject().asLiteral().getValue());
				else if (st.getObject().isResource()) 
					doc.addField("dcterms_date_vocab", st.getObject().asResource().getURI());
			}
			for(Statement st:proxy.listProperties(DcTerms.creator).toList()) {
				if (st.getObject().isResource()) 
					doc.addField("dcterms_creator_vocab", st.getObject().asResource().getURI());
			}
			for(Statement st:proxy.listProperties(DcTerms.contributor).toList()) {
				if (st.getObject().isResource()) 
					doc.addField("dcterms_contributor_vocab", st.getObject().asResource().getURI());
			}
			for(Statement st:proxy.listProperties(DcTerms.publisher).toList()) {
				if (st.getObject().isResource()) 
					doc.addField("dcterms_publisher_vocab", st.getObject().asResource().getURI());
			}
			for(Statement st:proxy.listProperties(DcTerms.subject).toList()) {
				if (st.getObject().isResource()) 
					doc.addField("dcterms_subject_vocab", st.getObject().asResource().getURI());
			}
			for(Statement st:proxy.listProperties(DcTerms.coverage).toList()) {
				if (st.getObject().isResource()) 
					doc.addField("dcterms_coverage_vocab", st.getObject().asResource().getURI());
			}
		}
		
		JSONObject record = new JSONObject();
		writePropertiesToJsonAndSolrDoc(cho, record, doc);

		JSONObject json = new JSONObject();
		json.put("ore_aggregation", agg);
		json.put("edm_providedCho", record);
		if (proxy!=null)
			json.put("ore_proxy", proxy);
		
		doc.addField("rossio_record", json.toJSONString());
//System.out.println(json.toJSONString());
		//		System.out.println(doc);
		solr.add(doc);
	}
	
	private void writePropertiesToJsonAndSolrDoc(Resource rdfResource, JSONObject jsonObj, SolrInputDocument solrDoc) {
		for(Statement st:rdfResource.listProperties().toList()) {
			JSONArray propValues = new JSONArray();
			for(String namespace: new String[] {DcTerms.NS, Edm.NS}) {
				String prefix=namespace.equals(DcTerms.NS) ? "dcterms" : "edm";
				if(st.getPredicate().getNameSpace().equals(namespace)) {
					if (st.getObject().isLiteral()) {
						if(solrDoc!=null)
							solrDoc.addField(prefix+"_"+st.getPredicate().getLocalName(), st.getObject().asLiteral().getValue());
	
						JSONObject value = new JSONObject();
						value.put("value", st.getObject().asLiteral().getValue());
						if(!StringUtils.isEmpty(st.getObject().asLiteral().getLanguage()))
							value.put("lang", st.getObject().asLiteral().getLanguage());
						
						propValues.add(value);
					} else if(st.getObject().isResource() && RdfUtil.isSeq(st.getObject().asResource())) {
						Seq seq = RdfUtil.getAsSeq(st.getObject().asResource());
						NodeIterator iter2 = seq.iterator();
					    while (iter2.hasNext()) {
					    	RDFNode node = iter2.next();
					    	if (node.isLiteral()) {
								Literal litValue = node.asLiteral();
						    	if(solrDoc!=null)
						    		solrDoc.addField(prefix+"_"+st.getPredicate().getLocalName(), litValue.getValue());
						    	
							    JSONObject value = new JSONObject();
							    value.put("value", litValue.getValue());
							    if(!StringUtils.isEmpty(litValue.getLanguage()))
							    	value.put("lang", litValue.getLanguage());
							    propValues.add(value);
					    	} else if(node.isResource())  {
					    		JSONObject value = new JSONObject();
					    		value.put("value", node.asResource().getURI());
					    		propValues.add(value);							    
					    	}
					    }
					} else if(st.getObject().isResource())  {
						JSONObject value = new JSONObject();
						value.put("value", st.getObject().asResource().getURI());
						propValues.add(value);
					}
					jsonObj.put(st.getPredicate().getLocalName(), propValues);
					break;
				}
			}
		}
	}


	public void commit() throws SolrServerException, IOException {
		solr.commit();
	}


	public void removeAllFrom(String source) throws SolrServerException, IOException {
		try {
			solr.deleteByQuery("rossio_source:"+ClientUtils.escapeQueryChars(source));
			commit();
		} catch (RemoteSolrException e) {
			if(!e.getMessage().contains("undefined field rossio_source"))
				throw e;
		}
	}

	public IndexingReport indexSourceFromRepository(String source, RepositoryWithSolr repository, String vocabsSparqlEndpoint, Logger log) {
		final EnrichmentTask enrichmentTask;
		if(runEnrichment) 
			enrichmentTask=EnrichmentTask.newInstanceForRossio(vocabsSparqlEndpoint);
		else
			enrichmentTask=null;
			
		IndexingReport report=new IndexingReport();
		try {
			removeAllFrom(source);
			repository.getItemsInSourceVersionAtSource(source, new ItemHandler() {
				@Override
				public boolean handle(String uuid, String idAtSource, String lastUpdate, byte[] content) throws Exception {
					try {
						RDFParser reader = RDFParser.create().lang(Lang.RDFTHRIFT).source(new ByteArrayInputStream(content)).build();
						Model model = Jena.createModel();
						reader.parse(model);
						String choUri=Rossio.NS_ITEM+uuid;
//						RdfUtil.printOutRdf(model);
//						addItem(source, model.createResource(Rossio.NS_ITEM+uuid));
						if(runEnrichment) {
							enrichmentTask.runOnRecord(model.createResource(choUri));
							//DEBUG
//							RdfUtil.printOutRdf(model);
							
							RDFWriter writer = RDFWriter.create().lang(Lang.RDFTHRIFT).source(model.getGraph()).build();
							ByteArrayOutputStream outstream=new ByteArrayOutputStream();
							writer.output(outstream);
							repository.updateItem(uuid, source, idAtSource, content, outstream.toByteArray());
						}
						addItem(source, model, choUri);
						report.incRecord();
						if(commitInterval>0 && 	report.getRecordCount() % commitInterval == 0) {
							commit();
							repository.commit();
						}
						return true;
					} catch (SolrServerException e) {
						report.addErrorOnRecord(uuid, e.getMessage());
						throw e;
					} catch (IOException e) {
						report.addErrorOnRecord(uuid, e.getMessage());
						throw e;
					}
				}
			});
			commit();
			repository.commit();
			report.finish();
		} catch (Exception e) {
			report.failure(e);
		}
		return report;
	}


	public void setCommitInterval(int commitInterval) {
		this.commitInterval=commitInterval;
	}
	
	public void setRunEnrichment(boolean runEnrichment) {
		this.runEnrichment = runEnrichment;
	}
	
	
}
