package rossio.ingest.preprocess;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Seq;
import org.apache.jena.rdf.model.Statement;
import org.w3c.dom.Element;

import rossio.data.models.DcTerms;
import rossio.data.models.Rossio;
import rossio.ingest.solr.RossioRecord;
import rossio.util.RdfUtil;

public class FcgPreprocessor implements MetadataPreprocessor {
	
	@Override
	public Model preprocess(String uuid, String sourceId, String dataProviderUri, Element metadata) {
		Model model = RossioRecord.fromOaidcToRossio(uuid, sourceId, dataProviderUri, metadata);
		String providedChoUri=Rossio.NS_ITEM+uuid;
		Resource cho = model.createResource(providedChoUri);
		
//		for(Property toCleanProp: new Property[] { DcTerms.title, DcTerms.subject, DcTerms.description}) {
//  		for(Statement st:cho.listProperties(toCleanProp).toList()) {
  		  for(Statement st:cho.listProperties().toList()) {
  			if (st.getObject().isLiteral()) {
  			  String val = st.getObject().asLiteral().getString();
  			  String valClean= removeChars(val);
  			  if(val.length()!=valClean.length())
  			    st.changeObject(model.createLiteral(valClean));				
  			} else if(st.getObject().isResource() && RdfUtil.isSeq(st.getObject().asResource())) {
  				Seq seq = RdfUtil.getAsSeq(st.getObject().asResource());
  				for (int i=1; i<=seq.size() ; i++) {
  					RDFNode node = seq.getObject(i);
  			    	if (node.isLiteral()) {
  			    		seq.set(i, model.createLiteral(removeChars(node.asLiteral().getString())));
  			    	}
  			    }
  			} 
  		}
//		}
		return model;
	}

	private static String removeChars(String text) {
	  return text.replaceAll("\\ˆ", "").replaceAll("‰", "");
	}
	

	@Override
	public Model preprocess(String uuid, String sourceId, String dataProviderUri, CSVRecord metadata) {
		throw new NotImplementedException();
	}
	
	public static void main(String[] args) throws Exception {
		String test="ˆO ‰ˆ\"‰antigoˆ\"‰ e o ˆ\"‰modernoˆ\"‰ no cinema";
		System.out.println(removeChars(test));	
	}
}
