package rossio.ingest.preprocess;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.NotImplementedException;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.vocabulary.DCTerms;
import org.w3c.dom.Element;

import rossio.data.models.DcTerms;
import rossio.data.models.Rossio;
import rossio.ingest.solr.RossioRecord;

public abstract class SwitchDcFieldPreprocessor implements MetadataPreprocessor {
	 protected Property from;
	 protected Property to;
	
	public SwitchDcFieldPreprocessor() {
		init();
	}
	
	protected abstract void init() ;
	
	protected void moveProperty(Resource cho) {
		Model model = cho.getModel();
		List<Statement> list = new ArrayList<>(cho.listProperties().toList());
		for(Statement st:list) {
			if(st.getPredicate().equals(from)) {
				model.add(model.createStatement(st.getSubject(), to, st.getObject()));
				model.remove(st);
			}
		}
	}
	
	
	@Override
	public Model preprocess(String uuid, String sourceId, String dataProviderUri, Element metadata) {
		Model model = RossioRecord.fromOaidcToRossio(uuid, sourceId, dataProviderUri, metadata);
		String providedChoUri=Rossio.NS_ITEM+uuid;
		Resource cho = model.createResource(providedChoUri);
		
		moveProperty(cho);
		return model;
	}
	

	@Override
	public Model preprocess(String uuid, String sourceId, String dataProviderUri, CSVRecord metadata) {
		throw new NotImplementedException();
	}
}
