package rossio.ingest.solr.manager;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.DayOfWeek;
import java.util.Date;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.transform.TransformerException;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.oclc.oai.harvester2.verb.ListSets;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import rossio.data.models.Dcat;
import rossio.data.models.Rdf;
import rossio.data.models.Rdfs;
import rossio.data.models.Rossio;
import rossio.ingest.preprocess.MetadataPreprocessor;
import rossio.oaipmh.OaiWrappedException;
import rossio.util.XmlUtil;
import rossio.util.RdfUtil.Jena;

public class MetadataSource{
	public enum Periodicity {
		NONE, WEEKLY, MONTHLY
	}
	
	public enum IngestMethod {
		OAIPMH, File
	}
	
	public static final String NS_INGESTAO=Rossio.NS+"ingestao/";
	public static final Property dataProviderProp=Jena.createProperty(NS_INGESTAO+"dataProvider");
	public static final Property oaiBaseUrlProp=Jena.createProperty(NS_INGESTAO+"oaiBaseUrl");
	public static final Property oaiSetProp=Jena.createProperty(NS_INGESTAO+"oaiSet");
	public static final Property oaiMetadataPrefixProp=Jena.createProperty(NS_INGESTAO+"oaiMetadataPrefix");
	public static final Property harvestingStatusProp=Jena.createProperty(NS_INGESTAO+"harvestingStatus");
	public static final Property lastResumptionTokenProp=Jena.createProperty(NS_INGESTAO+"lastResumptionToken");
	public static final Property metadataPreprocessorProp=Jena.createProperty(NS_INGESTAO+"metadataPreprocessor");
	public static final Property ingestMethodProp=Jena.createProperty(NS_INGESTAO+"ingestMethod");
	public static final Property fileToIngestProp=Jena.createProperty(NS_INGESTAO+"fileToIngest");

	public static final Property lastHarvestTimestampProp=Jena.createProperty(NS_INGESTAO+"lastHarvestTimestamp");
	public static final Property harvestPeriodicityProp=Jena.createProperty(NS_INGESTAO+"harvestPeriodicity");
//	public static final Property harvestDayOfWeekProp=Jena.createProperty(NS_INGESTAO+"lastResumptionToken");
	
	private static final Pattern newUriPattern=Pattern.compile("\\/(new|novo)[^\\/]*$");
	
	public String uri;
	public String dataProvider;
	public String baseUrl;
	public String set;
	public String metadataPrefix;
	public TaskStatus status=null;
	public String resumptionToken="";
	public String name="";
	public IngestMethod ingestMethod=IngestMethod.OAIPMH;
	public File fileToIngest=null;
	
	public MetadataPreprocessor preprocessor=null;
	
	public Date lastHarvestTimestamp;
	public Periodicity harvestPeriodicity;
//	public DayOfWeek harvestDayOfWeek;

	public MetadataSource(String unparsed) {
		String[] split = unparsed.split("\\|");
		dataProvider=split[0];
		baseUrl=split[1];
		set=split[2];
		metadataPrefix=split[3];
		if(split.length>4)
			status=StringUtils.isEmpty(split[4]) ? null : TaskStatus.valueOf(split[4]);
//		if(split.length>5)
//			statusIndexing=split[5];
		if(split.length>6)
			resumptionToken=split[6];
		if(StringUtils.isEmpty(set))
			set=null;
		if(StringUtils.isEmpty(metadataPrefix))
			metadataPrefix="oai_dc";
		if(StringUtils.isEmpty(resumptionToken))
			resumptionToken=null;
		harvestPeriodicity=Periodicity.NONE;
	}
	public MetadataSource(Resource dsRes) {
		uri=dsRes.getURI();
		Matcher newUriMatcher=newUriPattern.matcher(uri);
		if(newUriMatcher.find()) {
			uri=Rossio.NS_CONJUNTO_DE_DADOS + UUID.randomUUID().toString();
		}
		
		dataProvider=dsRes.getProperty(dataProviderProp).getObject().asResource().getURI();
		Statement baseUrlSt = dsRes.getProperty(oaiBaseUrlProp);
		if(baseUrlSt!=null)
			baseUrl=baseUrlSt.getObject().asResource().getURI();
		Statement oaiMetadataPrefixSt = dsRes.getProperty(oaiMetadataPrefixProp);
		if(oaiMetadataPrefixSt!=null)
			metadataPrefix=oaiMetadataPrefixSt.getObject().asLiteral().getString();
		Statement setProperty = dsRes.getProperty(oaiSetProp);
		if(setProperty==null)
			set=null;
		else {
			set=setProperty.getObject().asLiteral().getString();
			if(StringUtils.isEmpty(set))
				set=null;
		}
		Statement lastTokenSt = dsRes.getProperty(lastResumptionTokenProp);
		if(lastTokenSt!=null)
			resumptionToken=lastTokenSt.getObject().asLiteral().getString();
		else
			resumptionToken="";
		
		Statement preprocessorSt = dsRes.getProperty(metadataPreprocessorProp);
		if(preprocessorSt!=null) {
			String preprocessorClassName=preprocessorSt.getObject().asLiteral().getString();
			try {
				preprocessor=(MetadataPreprocessor) Class.forName(preprocessorClassName).newInstance();
			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
				throw new RuntimeException(e.getMessage(), e);
			}
		}

		Statement ingestMethodSt = dsRes.getProperty(ingestMethodProp);
		if(ingestMethodSt!=null) 
			ingestMethod=IngestMethod.valueOf(ingestMethodSt.getObject().asLiteral().getString());
				
		Statement fileToIngestSt = dsRes.getProperty(fileToIngestProp);
		if(fileToIngestSt!=null) {
			String filePath = fileToIngestSt.getObject().asLiteral().getString();
			if(!StringUtils.isEmpty(filePath))
				fileToIngest=new File(filePath);
		}		
		Statement statusSt=dsRes.getProperty(harvestingStatusProp);
		if( statusSt!=null && !(StringUtils.isEmpty(statusSt.getObject().asLiteral().getString())))
			status=TaskStatus.valueOf(statusSt.getObject().asLiteral().getString());
		name=dsRes.getProperty(Rdfs.label).getObject().asLiteral().getString();
		
		Statement periodicitySt = dsRes.getProperty(harvestPeriodicityProp);
		harvestPeriodicity=periodicitySt==null ? Periodicity.NONE : Periodicity.valueOf(periodicitySt.getObject().asLiteral().getString());
//		Statement harvestDayOfWeekSt = dsRes.getProperty(harvestDayOfWeekProp);
//		harvestDayOfWeek=harvestDayOfWeekSt==null ? DayOfWeek.MONDAY : DayOfWeek.valueOf(harvestDayOfWeekSt.getObject().asLiteral().getString());
		
		Statement lastHarvestSt = dsRes.getProperty(lastHarvestTimestampProp);
		if(lastHarvestSt!=null) {
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
			try {
				lastHarvestTimestamp=dateFormat.parse(lastHarvestSt.getObject().asLiteral().getString());
			} catch (ParseException e) {
				throw new RuntimeException(e.getMessage(), e);
			}
		}
	}
	

	public String getSourceId() {
		if(baseUrl!=null)
			return getSourceIdDeprecated();
//		return fileToIngest.getParentFile().getName()+"/"+fileToIngest.getName();
		return uri;
	}
	public String getSourceIdDeprecated() {
		return baseUrl+"#"+(set==null ? "" : set);
	}

	public void updateStatus(String result) {
		if (result.startsWith("SUCCESS"))
			status=TaskStatus.SUCCESS;
		else
			status=TaskStatus.FAILURE;
	}

	public String serializeToString() {
		return dataProvider+"|"+baseUrl+"|"+(set==null ? "" : set)+"|"
			+(metadataPrefix==null ? "" : metadataPrefix)+"|"
			+(status==null ? "" : status)+"|"
//			+(statusIndexing==null ? "" : statusIndexing)+"|"
			+(resumptionToken==null ? "" : resumptionToken);
	}
	

	public Resource toRdf(Model m) {
		Resource res = m.createResource(uri);
		res.addProperty(Rdf.type, Dcat.Dataset);
		res.addProperty(dataProviderProp, m.createResource(dataProvider));
		if(baseUrl!=null)
			res.addProperty(oaiBaseUrlProp, m.createResource(baseUrl));
		if(metadataPrefix!=null)
			res.addProperty(oaiMetadataPrefixProp, metadataPrefix);
		res.addProperty(oaiSetProp, set==null? "" : set);
		res.addProperty(lastResumptionTokenProp, resumptionToken==null? "" : resumptionToken);
//		res.addProperty(indexingStatusProp, statusIndexing);
		if (preprocessor!=null)
			res.addProperty(metadataPreprocessorProp, preprocessor.getClass().getCanonicalName()); 
		if (status!=null)
			res.addProperty(harvestingStatusProp, status.toString()); 
		if (ingestMethod!=null)
			res.addProperty(ingestMethodProp, ingestMethod.name()); 
		if (fileToIngest!=null)
			res.addProperty(fileToIngestProp, fileToIngest.getAbsolutePath()); 
		res.addProperty(Rdfs.label, name==null? "" : name);
		
//		res.addProperty(harvestDayOfWeekProp, harvestDayOfWeek.toString());
		res.addProperty(harvestPeriodicityProp, harvestPeriodicity.toString());
		if(lastHarvestTimestamp!=null) {
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
			res.addProperty(lastHarvestTimestampProp, 
					dateFormat.format(lastHarvestTimestamp),
					XSDDatatype.XSDdateTime);
		}
		return res;
	}
	
	public void updateName() throws OaiWrappedException, TransformerException {
		if(set!=null) {
			ListSets ls=new ListSets(baseUrl);
            NodeList nodeList = ls.getNodeList("/oai20:OAI-PMH/oai20:ListSets/oai20:set");
            for(int i=0; i<nodeList.getLength(); i++) {
            	Node item = nodeList.item(i);
            	if(set.equals(XmlUtil.getText(XmlUtil.getElementByTagNameIgnoreCase((Element)item, "setSpec")))) {
        			name=XmlUtil.getText(XmlUtil.getElementByTagNameIgnoreCase((Element)item, "setName"));
					break;
            	}
            }
		}
	}

	public boolean todoHarvest() {
		if(lastHarvestTimestamp==null)
			return true;
		if(harvestPeriodicity.equals(Periodicity.NONE))
			return false;
		if(harvestPeriodicity.equals(Periodicity.MONTHLY)) {
			 return new Date().after(DateUtils.addMonths(lastHarvestTimestamp, 1));
		}
		if(harvestPeriodicity.equals(Periodicity.WEEKLY)) 
			return new Date().after(DateUtils.addWeeks(lastHarvestTimestamp, 1));
		return false;
	}

}