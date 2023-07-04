package rossio.ingest.solr;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import javax.xml.stream.XMLStreamReader;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFWriter;
import org.apache.solr.client.solrj.SolrServerException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import rossio.ingest.solr.manager.Logger;
import rossio.ingest.solr.manager.MetadataSource;
import rossio.ingest.solr.manager.MetadataSources;
import rossio.oaipmh.HarvestException;
import rossio.oaipmh.HarvestReport;
import rossio.oaipmh.OaiPmhRecord;
import rossio.oaipmh.OaipmhHarvest;
import rossio.oaipmh.OaipmhHarvestWithHandler;
import rossio.oaipmh.OaipmhHarvestWithHandler.Handler;
import rossio.util.RdfUtil;
import rossio.util.XmlStaxParseUtil;
import rossio.util.XmlUtil;

public class HarvestCsvFileSourceIntoSolr {
	RepositoryWithSolr harvestTo;

//	String baseUrl;
//	String set;
//	String metadataPrefix;
//	Date lastHarvestTimestamp;
	MetadataSource source;
	String resumptionToken;

//	String sourceId;
//	String dataProviderUri;

	int commitInterval;

	HarvestReport report;

//	public HarvestOaiSourceIntoSolrWithHandler(String sourceId, String dataProviderUri, String baseUrl, String set, String metadataPrefix, Date lastHarvestTimestamp, RepositoryWithSolr harvestTo) {
//		super();
//		this.harvestTo = harvestTo;
//		this.baseUrl = baseUrl;
//		this.set = set;
//		this.metadataPrefix = metadataPrefix;
//		this.sourceId = sourceId;
//		this.dataProviderUri = dataProviderUri;
//		this.lastHarvestTimestamp = lastHarvestTimestamp;
//	}

	public HarvestCsvFileSourceIntoSolr(MetadataSource src, RepositoryWithSolr repository) {
		this.harvestTo = repository;
		this.source = src;
	}

	public HarvestReport run(Logger log, MetadataSources sources, MetadataSource src) throws HarvestException {
		return run(null, log, sources, src);
	}

	public HarvestReport run(Integer maxRecords, Logger log, MetadataSources sources, MetadataSource src) {
		int maximumRetries = 3;
		int retry = 0;
		report = new HarvestReport();
		;
//        try {
//			harvestTo.removeAllFrom(source.getSourceId());
//		} catch (SolrServerException e2) {
//			e2.printStackTrace();
//		} catch (IOException e2) {
//			e2.printStackTrace();
//		}

		
		
		try {
			FileInputStream fis=new FileInputStream(src.fileToIngest);
			InputStreamReader fileReader=new InputStreamReader(fis, StandardCharsets.UTF_8);
			CSVParser csvParser=new CSVParser(fileReader, CSVFormat.DEFAULT);

			for(CSVRecord rec: csvParser) {
				try {
					report.incRecord();
					String uuid = UUID.randomUUID().toString();
					Model mdRdf = source.preprocessor.preprocess(uuid, source.getSourceId(),
							source.dataProvider, rec);
					harvestTo.addItem(uuid, source.getSourceId(), uuid, RdfUtil.serializeToRdfRift(mdRdf));
				} catch (SolrServerException e) {
					errorOnRecord("by file", e);
					break;
				} catch (IOException e) {
					errorOnRecord("by file", e);
					break;
				}
			}
			harvestTo.end();
		} catch (Exception e) {
			log.log("WARN: Harvest failed. Cause: " + ExceptionUtils.getStackTrace(e));
			report.failure("Harvest failed. Cause:", e);
		}
		return report;
	}

	private void errorOnRecord(String recId, Exception e) {
		report.addErrorOnRecord(recId, e.getMessage());
	}

	private byte[] serializeToRossioRdfRift(String uuid, Element metadata) {
		Model m = RossioRecord.fromOaidcToRossio(uuid, source.getSourceId(), source.dataProvider, metadata);
//		RdfUtil.printOutRdf(m);
		return RdfUtil.serializeToRdfRift(m);
	}

	public void setCommitInterval(int commitInterval) {
		this.commitInterval = commitInterval;
	}

	public void resumeWithToken(String resumptionToken) {
		this.resumptionToken = resumptionToken;
	}

}
