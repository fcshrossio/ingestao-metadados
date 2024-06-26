package rossio.ingest.preprocess;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.NotImplementedException;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.vocabulary.DCTerms;
import org.w3c.dom.Element;

import rossio.data.models.Dc;
import rossio.data.models.DcTerms;
import rossio.data.models.Rossio;
import rossio.ingest.solr.RossioRecord;

public class SwitchSubjectToDescriptionPreprocessor extends SwitchDcFieldPreprocessor {
	@Override
	protected void init() {
		from=DcTerms.subject;
		to=DcTerms.description;
	}
}
