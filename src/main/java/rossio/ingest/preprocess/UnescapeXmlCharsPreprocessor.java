package rossio.ingest.preprocess;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Seq;
import org.apache.jena.rdf.model.Statement;
import org.htmlcleaner.HtmlCleaner;
import org.w3c.dom.Element;

import rossio.data.models.DcTerms;
import rossio.data.models.Rossio;
import rossio.ingest.solr.RossioRecord;
import rossio.util.RdfUtil;

public class UnescapeXmlCharsPreprocessor implements MetadataPreprocessor {
	
	@Override
	public Model preprocess(String uuid, String sourceId, String dataProviderUri, Element metadata) {
		Model model = RossioRecord.fromOaidcToRossio(uuid, sourceId, dataProviderUri, metadata);
		String providedChoUri=Rossio.NS_ITEM+uuid;
		Resource cho = model.createResource(providedChoUri);
		
		for(Statement st:cho.listProperties().toList()) {
			if (st.getObject().isLiteral()) {
				st.changeObject(model.createLiteral(StringEscapeUtils.unescapeXml(st.getObject().asLiteral().getString())));				
			} else if(st.getObject().isResource() && RdfUtil.isSeq(st.getObject().asResource())) {
				Seq seq = RdfUtil.getAsSeq(st.getObject().asResource());
				for (int i=1; i<=seq.size() ; i++) {
					RDFNode node = seq.getObject(i);
			    	if (node.isLiteral()) {
			    		seq.set(i, model.createLiteral(StringEscapeUtils.unescapeXml(node.asLiteral().getString())));
			    	}
			    }
			} 
		}
		return model;
	}


	@Override
	public Model preprocess(String uuid, String sourceId, String dataProviderUri, CSVRecord metadata) {
		throw new NotImplementedException();
	}
	
	public static void main(String[] args) throws Exception {
		String test="Di, pues vienes de l&#039;aldea\n &lt;p&gt;Tem outro esboço, no verso.&lt;/p&gt;";
		CharSequence cleaned=new HtmlCleaner().clean(test).getText();				
		System.out.println(cleaned.toString());	
		
		System.out.println(StringEscapeUtils.unescapeXml(test)); 
		
	}
}
