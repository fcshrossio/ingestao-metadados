package rossio.ingest.datasets.dspace;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;

import rossio.data.models.Dcat;
import rossio.data.models.Rdf;
import rossio.util.RdfUtil;


public class DcatToDctermsConverter {
	public static DcMetadata convert(Model dcat) {
		DcMetadata dc=new DcMetadata();
		Resource datasetRes = RdfUtil.findFirstResourceWithProperties(dcat, Rdf.type, Dcat.Dataset, null, null);
		if(datasetRes==null) 
			return null;
		for (Statement st : datasetRes.listProperties().toList()) 
			dc.addValue(st);			
		return dc;
	}

}
