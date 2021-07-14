package rossio.ingest.solr.manager;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.DayOfWeek;
import java.util.Date;

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
import rossio.oaipmh.OaiWrappedException;
import rossio.util.XmlUtil;
import rossio.util.RdfUtil.Jena;

public class OaiSourceIndexStatus{
	
	public static final Property lastIndexTimestampProp=Jena.createProperty(OaiSource.NS_INGESTAO+"lastIndexTimestamp");
	public static final Property indexingStatusProp=Jena.createProperty(OaiSource.NS_INGESTAO+"indexingStatus");
	
	OaiSource oaiSource; 
	
	public Date lastIndexTimestamp;
	public TaskStatus status=null;
	
	public OaiSourceIndexStatus(Resource dsRes, OaiSource oaiSource) {
		this.oaiSource = oaiSource;
		
		Statement lastIndexSt = dsRes.getProperty(lastIndexTimestampProp);
		if(lastIndexSt!=null) {
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
			try {
				lastIndexTimestamp=dateFormat.parse(lastIndexSt.getObject().asLiteral().getString());
			} catch (ParseException e) {
				throw new RuntimeException(e.getMessage(), e);
			}
		}
		Statement statusSt=dsRes.getProperty(indexingStatusProp);
		if( statusSt!=null)
			status=TaskStatus.valueOf(statusSt.getObject().asLiteral().getString());
	}

	public Resource toRdf(Model m) {
		Resource res = m.createResource(oaiSource.uri);
		res.addProperty(Rdf.type, Dcat.Dataset);		
		if(lastIndexTimestamp!=null) {
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
			res.addProperty(lastIndexTimestampProp, 
					dateFormat.format(lastIndexTimestamp),
					XSDDatatype.XSDdateTime);
		}
		if (status!=null)
			res.addProperty(indexingStatusProp, status.toString()); 
		return res;
	}
	
	public void updateStatus(String result) {
		if (result.startsWith("SUCCESS")) {
			status=TaskStatus.SUCCESS;
			lastIndexTimestamp=oaiSource.lastHarvestTimestamp;
		} else
			status=TaskStatus.FAILURE;
	}

	public boolean todoIndex() {
		if(oaiSource==null || oaiSource.status!=TaskStatus.SUCCESS)
			return false;
		if(status!=null && status==TaskStatus.PAUSED)
			return false;
		if(lastIndexTimestamp==null)
			return true;
		return lastIndexTimestamp.before(oaiSource.lastHarvestTimestamp);
	}

	public String getSourceId() {
		return oaiSource.getSourceId();
	}

}