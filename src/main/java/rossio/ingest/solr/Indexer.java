package rossio.ingest.solr;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.Date;
import java.util.Random;

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
import rossio.ingest.solr.RepositoryWithSolr.FetchOption;
import rossio.ingest.solr.RepositoryWithSolr.ItemHandler;
import rossio.ingest.solr.manager.Logger;
import rossio.ingest.solr.manager.OaiSourceIndexStatus;
import rossio.util.DevelopementSingleton;
import rossio.util.RdfUtil;
import rossio.util.RdfUtil.Jena;
import rossio.util.ThreadedRunner;

public class Indexer {
	SolrClient solr;
	boolean runEnrichment = true;
	int commitInterval = 20000;

	long lastLog = 0;
	long logInterval = 3600000;// 1 hour

	public Indexer(String solrUrl) {
//		final String solrUrl = "http://localhost:8983/solr";
//		solr= new HttpSolrClient.Builder(solrUrl)
//		    .withConnectionTimeout(10000)
//		    .withSocketTimeout(60000)
//		    .build();
		solr = new ConcurrentUpdateSolrClient.Builder(solrUrl).withConnectionTimeout(10000).withSocketTimeout(60000)
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
		Resource aggregation = model.createResource(providedChoUri + "#aggregation");
		Resource proxy = RdfUtil.getResourceIfExists(cho.getURI() + "#proxy", model);

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
		if (proxy != null) {
			proxyJson = new JSONObject();
			writePropertiesToJsonAndSolrDoc(proxy, proxyJson, null);

			// add normalized dates to a particular solr field for date ranges
			for (Statement st : proxy.listProperties(DcTerms.date).toList()) {
				doc.addField("date_range", RdfUtil.getUriOrLiteralValue(st.getObject()));
			}
			// add normalized languages to a particular solr field
			for (Statement st : proxy.listProperties(DcTerms.language).toList()) {
				doc.addField("dcterms_language_vocab", RdfUtil.getUriOrLiteralValue(st.getObject()));
			}
			for (Statement st : proxy.listProperties(DcTerms.creator).toList()) {
				if (st.getObject().isResource())
					doc.addField("dcterms_creator_vocab", st.getObject().asResource().getURI());
			}
			for (Statement st : proxy.listProperties(DcTerms.contributor).toList()) {
				if (st.getObject().isResource())
					doc.addField("dcterms_contributor_vocab", st.getObject().asResource().getURI());
			}
			for (Statement st : proxy.listProperties(DcTerms.publisher).toList()) {
				if (st.getObject().isResource())
					doc.addField("dcterms_publisher_vocab", st.getObject().asResource().getURI());
			}
			for (Statement st : proxy.listProperties(DcTerms.subject).toList()) {
				if (st.getObject().isResource())
					doc.addField("dcterms_subject_vocab", st.getObject().asResource().getURI());
			}
			for (Statement st : proxy.listProperties(DcTerms.coverage).toList()) {
				if (st.getObject().isResource())
					doc.addField("dcterms_coverage_vocab", st.getObject().asResource().getURI());
			}
			for (Statement st : proxy.listProperties(DcTerms.type).toList()) {
				if (st.getObject().isResource())
					doc.addField("dcterms_type_vocab", st.getObject().asResource().getURI());
			}
		}

		JSONObject record = new JSONObject();
		writePropertiesToJsonAndSolrDoc(cho, record, doc);

		JSONObject json = new JSONObject();
		json.put("ore_aggregation", agg);
		json.put("edm_providedCho", record);
		if (proxyJson != null)
			json.put("ore_proxy", proxyJson);

		doc.addField("rossio_record", json.toJSONString());
//System.out.println(json.toJSONString());
		// System.out.println(doc);
		solr.add(doc);
	}

	private void writePropertiesToJsonAndSolrDoc(Resource rdfResource, JSONObject jsonObj, SolrInputDocument solrDoc) {
		for (Statement st : rdfResource.listProperties().toList()) {
			JSONArray propValues = new JSONArray();
			for (String namespace : new String[] { DcTerms.NS, Edm.NS }) {
				String prefix = namespace.equals(DcTerms.NS) ? "dcterms" : "edm";
				if (st.getPredicate().getNameSpace().equals(namespace)) {
					if (st.getObject().isLiteral()) {
						if (solrDoc != null)
							solrDoc.addField(prefix + "_" + st.getPredicate().getLocalName(),
									st.getObject().asLiteral().getValue());

						JSONObject value = new JSONObject();
						value.put("value", st.getObject().asLiteral().getValue());
						if (!StringUtils.isEmpty(st.getObject().asLiteral().getLanguage()))
							value.put("lang", st.getObject().asLiteral().getLanguage());

						propValues.add(value);
					} else if (st.getObject().isResource() && RdfUtil.isSeq(st.getObject().asResource())) {
						Seq seq = RdfUtil.getAsSeq(st.getObject().asResource());
						NodeIterator iter2 = seq.iterator();
						while (iter2.hasNext()) {
							RDFNode node = iter2.next();
							if (node.isLiteral()) {
								Literal litValue = node.asLiteral();
								if (solrDoc != null)
									solrDoc.addField(prefix + "_" + st.getPredicate().getLocalName(),
											litValue.getValue());

								JSONObject value = new JSONObject();
								value.put("value", litValue.getValue());
								if (!StringUtils.isEmpty(litValue.getLanguage()))
									value.put("lang", litValue.getLanguage());
								propValues.add(value);
							} else if (node.isResource()) {
								JSONObject value = new JSONObject();
								value.put("value", node.asResource().getURI());
								propValues.add(value);
							}
						}
					} else if (st.getObject().isResource()) {
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
			solr.deleteByQuery("rossio_source:" + ClientUtils.escapeQueryChars(source));
			commit();
		} catch (RemoteSolrException e) {
			if (!e.getMessage().contains("undefined field rossio_source"))
				throw e;
		}
	}

	public IndexingReport indexSourceFromRepository(OaiSourceIndexStatus source, RepositoryWithSolr repository,
			String vocabsSparqlEndpoint, Logger log) {
		final Random random = new Random();
		final EnrichmentTask enrichmentTask;
		String sourceId=source.getSourceId();
		
		if (runEnrichment)
			enrichmentTask = EnrichmentTask.newInstanceForRossio(source, vocabsSparqlEndpoint);
		else
			enrichmentTask = null;

		IndexingReport report = new IndexingReport();
		try {
			removeAllFrom(sourceId);
			lastLog = new Date().getTime();

			ThreadedRunner runner = new ThreadedRunner(10);
			final boolean[] haltProcessing = new boolean[] { false };

			repository.getItemsInSource(sourceId, source.isEnriched() ? FetchOption.VERSION_AT_ROSSIO : FetchOption.VERSION_AT_SOURCE, new ItemHandler() {
				@Override
				public boolean handle(String uuid, String idAtSource, String lastUpdate, byte[] content,
						byte[] contentRossio) throws Exception {
					if (haltProcessing[0])
						return false;
					runner.run(new Runnable() {
						@Override
						public void run() {
							try {
								RDFParser reader = RDFParser.create().lang(Lang.RDFTHRIFT)
										.source(new ByteArrayInputStream(source.isEnriched() ? contentRossio : content)).build();
								Model model = Jena.createModel();
								reader.parse(model);
								String choUri = Rossio.NS_ITEM + uuid;
//						RdfUtil.printOutRdf(model);
//						addItem(source, model.createResource(Rossio.NS_ITEM+uuid));

								Instant start = random.nextInt(100) >= 98 ? Instant.now() : null;
								Instant enriched = null;
								Instant added = null;

								if (runEnrichment && !source.isEnriched()) {
									enrichmentTask.runOnRecord(model.createResource(choUri));
									// DEBUG
//							RdfUtil.printOutRdf(model);

									RDFWriter writer = RDFWriter.create().lang(Lang.RDFTHRIFT).source(model.getGraph())
											.build();
									ByteArrayOutputStream outstream = new ByteArrayOutputStream();
									writer.output(outstream);
									repository.updateItem(uuid, sourceId, idAtSource, content, outstream.toByteArray());
								}

								if (start != null)
									enriched = Instant.now();

								addItem(sourceId, model, choUri);

								if (start != null) {
									added = Instant.now();
									log.log("Record ingesting times:" + (enriched.toEpochMilli() - start.toEpochMilli())
											+ " ms (enriched) " + (added.toEpochMilli() - enriched.toEpochMilli())
											+ "ms (added)");
								}

								report.incRecord();

								if (new Date().getTime() - lastLog > logInterval) {
									lastLog = new Date().getTime();
									log.log(sourceId + " - " + report.toLogStringIntermediate());
								}
								if (commitInterval > 0 && report.getRecordCount() % commitInterval == 0) {
									commit();
									if (runEnrichment && !source.isEnriched()) 
										repository.commit();
								}
							} catch (SolrServerException e) {
								report.addErrorOnRecord(uuid, e.getMessage());
//									throw e;
							} catch (IOException e) {
								report.addErrorOnRecord(uuid, e.getMessage());
//									throw e;
							}
						}
					});
					return true;
				}
			});
			runner.awaitTermination(5);
			commit();
			if (runEnrichment && !source.isEnriched()) 
				repository.commit();
			report.finish();
		} catch (Exception e) {
			report.failure(e);
		}
		return report;
	}

	public void setCommitInterval(int commitInterval) {
		this.commitInterval = commitInterval;
	}

	public void setRunEnrichment(boolean runEnrichment) {
		this.runEnrichment = runEnrichment;
	}

}
