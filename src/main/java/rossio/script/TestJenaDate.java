package rossio.script;

import java.io.File;
import java.io.FileOutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.oclc.oai.harvester2.verb.ListSets;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import rossio.data.models.DcTerms;
import rossio.data.models.Dcat;
import rossio.data.models.Foaf;
import rossio.data.models.Rdf;
import rossio.data.models.Rossio;
import rossio.ingest.solr.manager.MetadataSource;
import rossio.ingest.solr.manager.MetadataSources;
import rossio.util.RdfUtil;
import rossio.util.RdfUtil.Jena;
import rossio.util.XmlUtil;

public class TestJenaDate {

	public static void main(String[] args) throws Exception {
		Model m = Jena.createModel();
		
		
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		
		Resource res = m.createResource("http://nuno.org/", Foaf.Person);
		res.addProperty(DcTerms.date,
				dateFormat.format(new Date()),
				XSDDatatype.XSDdateTimeStamp
				);
		
		
		RdfUtil.printOutRdf(m);
		
//		Object parse = XSDDatatype.XSDdateTime.parse(res.getProperty(DcTerms.date).getObject().asLiteral().getLexicalForm());
		Object parse = XSDDatatype.XSDdateTimeStamp.parse(res.getProperty(DcTerms.date).getObject().asLiteral().getString());
		System.out.println(parse.getClass());
		System.out.println(parse);
		

	}
	
	
}
