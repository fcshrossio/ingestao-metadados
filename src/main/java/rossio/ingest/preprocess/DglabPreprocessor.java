package rossio.ingest.preprocess;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.NodeIterator;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Seq;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.solr.common.SolrInputDocument;
import org.htmlcleaner.HtmlCleaner;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.w3c.dom.Element;

import rossio.data.models.DcTerms;
import rossio.data.models.Edm;
import rossio.data.models.Ore;
import rossio.data.models.Rdf;
import rossio.data.models.Rossio;
import rossio.ingest.solr.RossioRecord;
import rossio.util.MapOfLists;
import rossio.util.RdfUtil;
import rossio.util.XmlUtil;
import rossio.util.RdfUtil.Jena;

public class DglabPreprocessor implements MetadataPreprocessor {
	private static Pattern imgPattern=Pattern.compile("<img[^>]+ src=\"([^\"]+)\"[^>]*>"); 
	
	@Override
	public Model preprocess(String uuid, String sourceId, String dataProviderUri, Element metadata) {
		Model model = RossioRecord.fromOaidcToRossio(uuid, sourceId, dataProviderUri, metadata);
		String providedChoUri=Rossio.NS_ITEM+uuid;
		Resource cho = model.createResource(providedChoUri);
		
		moveSubjectToDescription(cho);
		return model;
	}
	
	public static void moveSubjectToDescription(Resource cho) {
		Model model = cho.getModel();
		List<Statement> list = new ArrayList<>(cho.listProperties().toList());
		for(Statement st:list) {
			if(st.getPredicate().equals(DcTerms.subject)) {
				model.add(model.createStatement(st.getSubject(), DCTerms.description, st.getObject()));
				model.remove(st);
			}
		}
	}

	@Override
	public Model preprocess(String uuid, String sourceId, String dataProviderUri, CSVRecord metadata) {
		throw new NotImplementedException();
	}
}
