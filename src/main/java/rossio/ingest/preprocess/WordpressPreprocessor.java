package rossio.ingest.preprocess;

import java.util.ArrayList;

import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.NodeIterator;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Seq;
import org.apache.jena.rdf.model.Statement;
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

public class WordpressPreprocessor implements MetadataPreprocessor {

	@Override
	public Model preprocess(String uuid, String sourceId, String dataProviderUri, Element metadata) {
		Model model = RossioRecord.fromOaidcToRossio(uuid, uuid, uuid, metadata);
		String providedChoUri=Rossio.NS_ITEM+uuid;
		Resource cho = model.createResource(providedChoUri);
		
		for(Statement st:cho.listProperties().toList()) {
			if (st.getObject().isLiteral()) {
				CharSequence cleaned=new HtmlCleaner().clean(st.getObject().asLiteral().getString()).getText();				
				st.changeObject(model.createLiteral(cleaned.toString()));				
			} else if(st.getObject().isResource() && RdfUtil.isSeq(st.getObject().asResource())) {
				Seq newSeq=model.createSeq();
				Seq seq = RdfUtil.getAsSeq(st.getObject().asResource());
				NodeIterator iter2 = seq.iterator();
			    while (iter2.hasNext()) {
			    	RDFNode node = iter2.next();
			    	if (node.isLiteral()) {
			    		CharSequence cleaned=new HtmlCleaner().clean(node.asLiteral().getString()).getText();				
			    		newSeq.add(model.createLiteral(cleaned.toString()));
			    	} else
						newSeq.add(node);
			    }
			    st.changeObject(newSeq);				
			} 
		}
		return model;
	}

}
