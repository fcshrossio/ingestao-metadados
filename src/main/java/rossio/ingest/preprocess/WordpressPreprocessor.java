package rossio.ingest.preprocess;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.NotImplementedException;
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

public class WordpressPreprocessor implements MetadataPreprocessor {
	private static Pattern imgPattern=Pattern.compile("<img[^>]+ src=\"([^\"]+)\"[^>]*>"); 
	
	@Override
	public Model preprocess(String uuid, String sourceId, String dataProviderUri, Element metadata) {
		Model model = RossioRecord.fromOaidcToRossio(uuid, sourceId, dataProviderUri, metadata);
		String providedChoUri=Rossio.NS_ITEM+uuid;
		Resource cho = model.createResource(providedChoUri);
		
		for(Statement st:cho.listProperties().toList()) {
			if(st.getPredicate().equals(DcTerms.identifier) || st.getPredicate().equals(DcTerms.relation)) {
				if (st.getObject().isLiteral()) {
					Matcher matcher = imgPattern.matcher(st.getObject().asLiteral().getString());
					if(matcher.find()) 
						st.changeObject(model.createLiteral(matcher.group(1)));										
				} else if(st.getObject().isResource() && RdfUtil.isSeq(st.getObject().asResource())) {
//					Seq newSeq=model.createSeq();
					Seq seq = RdfUtil.getAsSeq(st.getObject().asResource());
					for (int i=1; i<=seq.size() ; i++) {
						RDFNode node = seq.getObject(i);
						if (node.isLiteral()) {
							Matcher matcher = imgPattern.matcher(node.asLiteral().getString());
							if(matcher.find()) 
								seq.set(i, model.createLiteral(matcher.group(1)));
						} 
					}
				} 
			}
		}
		for(Statement st:cho.listProperties().toList()) {
			if (st.getObject().isLiteral()) {
				CharSequence cleaned=new HtmlCleaner().clean(st.getObject().asLiteral().getString()).getText();				
				st.changeObject(model.createLiteral(cleaned.toString()));				
			} else if(st.getObject().isResource() && RdfUtil.isSeq(st.getObject().asResource())) {
				Seq seq = RdfUtil.getAsSeq(st.getObject().asResource());
				for (int i=1; i<=seq.size() ; i++) {
					RDFNode node = seq.getObject(i);
			    	if (node.isLiteral()) {
			    		CharSequence cleaned=new HtmlCleaner().clean(node.asLiteral().getString()).getText();				
			    		seq.set(i, model.createLiteral(cleaned.toString()));
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
}
