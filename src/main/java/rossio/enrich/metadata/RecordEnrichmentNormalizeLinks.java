package rossio.enrich.metadata;

import java.util.ArrayList;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;

import rossio.data.models.DcTerms;
import rossio.data.models.Edm;
import rossio.data.models.Ore;
import rossio.enrich.linkprovider.ChoLinksUtil;
import rossio.enrich.linkprovider.LinkTest;
import rossio.enrich.linkprovider.LinksReport;
import rossio.util.RdfUtil;

public class RecordEnrichmentNormalizeLinks implements RecordEnrichment {
	
	public RecordEnrichmentNormalizeLinks() {
	}
	
	@Override
	public void enrich(Resource cho) {
		LinksReport rep=new LinksReport();
		rep.item=cho;
		rep.tests = ChoLinksUtil.testAllLinks(cho, DcTerms.identifier);
	
		for( LinkTest test: rep.tests ) {
			String mime=test.getResult().toLowerCase();
			if(mime.startsWith("text/html")) {
				rep.setIsShownAtIfEmpty(test.getUrl());
			} else if(mime.startsWith("image/jpeg") || mime.startsWith("image/gif") || mime.startsWith("image/png")) {
				rep.setThumbnailAndIsShownByIfEmpty(test.getUrl());
			} else if(mime.startsWith("application/pdf")) {
				rep.setIsShownByIfEmpty(test.getUrl());
			}
		}
		
		if (rep.thumbnail ==null) {
			ArrayList<LinkTest> relations = ChoLinksUtil.testAllLinks(cho, DcTerms.relation);
			for( LinkTest test: relations ) {
				String mime=test.getResult().toLowerCase();
				if(mime.startsWith("image/jpeg") || mime.startsWith("image/gif") || mime.startsWith("image/png")) {
					rep.setThumbnailIfEmpty(test.getUrl());
					break;
				}
			}
		}
		
		
		if (rep.isShownAt !=null || rep.thumbnail !=null || rep.isShownBy !=null) {
			Model model = cho.getModel();
			Resource aggregation=RdfUtil.getResourceIfExists(cho.getURI()+"#aggregation", cho.getModel());
			
			if(aggregation==null) {
				aggregation=model.createResource(cho.getURI()+"#aggregation", Ore.Aggregation);
				aggregation.addProperty(Edm.aggregatedCHO, cho);
			}
			
			if (rep.isShownAt !=null) 
				aggregation.addProperty(Edm.isShownAt, model.createResource(rep.isShownAt));
			if (rep.isShownBy !=null) 
				aggregation.addProperty(Edm.isShownBy, model.createResource(rep.isShownBy));
			if (rep.thumbnail !=null) 
				aggregation.addProperty(Edm.object, model.createResource(rep.thumbnail));
			if (rep.hasView!=null)
		        for(String viewUrl: rep.hasView)	
		        	aggregation.addProperty(Edm.hasView, model.createResource(viewUrl));
		}
		
		
//		if (proxy!=null )
//			RdfUtil.printOutRdf(proxy.getModel());		
	}

	
	
}
