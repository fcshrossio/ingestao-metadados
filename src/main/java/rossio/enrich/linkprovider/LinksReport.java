package rossio.enrich.linkprovider;

import java.util.ArrayList;
import java.util.HashSet;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;

import rossio.data.models.DcTerms;
import rossio.data.models.Edm;
import rossio.util.RdfUtil;

public class LinksReport {
	static final String bibliotecaNacionalUri="http://vocabs.rossio.fcsh.unl.pt/agentes/c_cf7ed0ab";
	
	public ArrayList<LinkTest> tests=new ArrayList<LinkTest>();
	
	String dataProvider;
	public Resource item;
	
	public String isShownAt;
	public String isShownBy;
	public String thumbnail;
	public ArrayList<String> hasView;
	
	public LinksReport(Resource cho) {
		Resource aggregation=RdfUtil.getResourceIfExists(cho.getURI()+"#aggregation", cho.getModel());
		Statement dataProviderSt = aggregation.getProperty(Edm.dataProvider);
		dataProvider = dataProviderSt.getObject().asResource().getURI();
		
		if(dataProvider.equals(bibliotecaNacionalUri)) {
			ArrayList<LinkTest> tests=new ArrayList<LinkTest>();
			ArrayList<String> identifiers=ChoLinksUtil.listUrls(cho, DcTerms.identifier);
			for(String url: identifiers) {
				if(url.contains("/purl.pt/")) {
					if (url.contains("media/cover")) {
						setThumbnailIfEmpty(url);
					} else if(url.contains("media/")) {
						setIsShownByIfEmpty(url);
					} else {
						setIsShownAtIfEmpty(url);
					}
				}else
					tests.add( ChoLinksUtil.testLink(url));
			}
		} else		
			tests = ChoLinksUtil.testAllLinks(cho, DcTerms.identifier);
	}

	public void setIsShownAtIfEmpty(String isShownAt) {
		if(this.isShownAt==null)
			this.isShownAt = isShownAt;
	}
	
	public void setIsShownByIfEmpty(String isShownBy) {
		if(this.isShownBy==null)
			this.isShownBy = isShownBy;
		else {
			if(hasView==null)
				hasView=new ArrayList<String>();
			hasView.add(isShownBy);
		}
	}
	
	public void setThumbnailAndIsShownByIfEmpty(String thumbnail) {
		if(this.thumbnail==null)
			this.thumbnail = thumbnail;
//		else
		String thumbLc=thumbnail.toLowerCase();
		if(!thumbLc.contains("thumb"))
			setIsShownByIfEmpty(thumbnail);
	}
	public void setThumbnailIfEmpty(String thumbnail) {
		if(this.thumbnail==null)
			this.thumbnail = thumbnail;
	}

//	public void setLinkWithErrorIfEmpty(String linkWithError) {
//		if(this.linkWithError==null)
//			this.linkWithError = linkWithError;
//	}
	
//	public boolean isComplete() {
//		return isShownAt!=null && thumbnail!=null;
//	}
	
}
