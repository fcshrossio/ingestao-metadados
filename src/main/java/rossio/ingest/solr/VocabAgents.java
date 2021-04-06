package rossio.ingest.solr;

import java.io.InputStream;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;

import rossio.data.models.Skos;
import rossio.util.RdfUtil;
import rossio.util.RdfUtil.Jena;

public class VocabAgents {

	static Model agentsVocab;
	
	static {
		InputStream rdfInStream = VocabAgents.class.getClassLoader().getResourceAsStream("/vocab-agents.rdf");
		agentsVocab=Jena.createModel();
		agentsVocab.read(rdfInStream, null);
	}
	private VocabAgents() {
	}
	
	
	public static String getAgentPrefLabel(String agentUri ) {
		Resource agentRes = Jena.getResourceIfExists(agentUri, agentsVocab);
		if(agentRes==null)
			return null;
		Statement prefLabelSt = agentRes.getProperty(Skos.prefLabel);
		if(prefLabelSt==null)
			return null;
		return prefLabelSt.getObject().asLiteral().getString();
	}
}
