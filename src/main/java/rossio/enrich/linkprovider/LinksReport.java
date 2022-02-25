package rossio.enrich.linkprovider;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;

import rossio.data.models.DcTerms;
import rossio.data.models.Edm;
import rossio.util.RdfUtil;

public class LinksReport {
	static final String bibliotecaNacionalUri="http://vocabs.rossio.fcsh.unl.pt/agentes/c_cf7ed0ab";
	static final Set<String> dglabUris=new HashSet<String>() {{
		add("http://vocabs.rossio.fcsh.unl.pt/agentes/c_20311d92");
		add("http://vocabs.rossio.fcsh.unl.pt/agentes/c_438671c9");
		add("http://vocabs.rossio.fcsh.unl.pt/agentes/c_76e2d079");
		add("http://vocabs.rossio.fcsh.unl.pt/agentes/c_64c458fe");
		add("http://vocabs.rossio.fcsh.unl.pt/agentes/c_5672cf6f");
		add("http://vocabs.rossio.fcsh.unl.pt/agentes/c_4229e047");
		add("http://vocabs.rossio.fcsh.unl.pt/agentes/c_50a5323c");
		add("http://vocabs.rossio.fcsh.unl.pt/agentes/c_d69788b6");
		add("http://vocabs.rossio.fcsh.unl.pt/agentes/c_53e2aa91");
		add("http://vocabs.rossio.fcsh.unl.pt/agentes/c_f9e05c96");
		add("http://vocabs.rossio.fcsh.unl.pt/agentes/c_6fde27ba");
		add("http://vocabs.rossio.fcsh.unl.pt/agentes/c_6da5082f");
		add("http://vocabs.rossio.fcsh.unl.pt/agentes/c_95ebe093");
		add("http://vocabs.rossio.fcsh.unl.pt/agentes/c_129780c4");
		add("http://vocabs.rossio.fcsh.unl.pt/agentes/c_7054d2d4");
		add("http://vocabs.rossio.fcsh.unl.pt/agentes/c_f95496ad");
		add("http://vocabs.rossio.fcsh.unl.pt/agentes/c_819ef5cf");
		add("http://vocabs.rossio.fcsh.unl.pt/agentes/c_5062b87f");
	}};
	
	public ArrayList<LinkTest> tests=new ArrayList<LinkTest>();
	
	String dataProvider;
	public Resource item;
	
	public String isShownAt;
	public String isShownBy;
	public String thumbnail;
	public ArrayList<String> hasView;
	
	private LinksReport() {
	}

	public static LinksReport createReportForCho(Resource cho) {
		Resource aggregation=RdfUtil.getResourceIfExists(cho.getURI()+"#aggregation", cho.getModel());
		Statement dataProviderSt = aggregation.getProperty(Edm.dataProvider);
		LinksReport report=new LinksReport();
		report.dataProvider = dataProviderSt.getObject().asResource().getURI();
		
		if(report.dataProvider.equals(bibliotecaNacionalUri)) {
			report.tests=new ArrayList<LinkTest>();
			ArrayList<String> identifiers=ChoLinksUtil.listUrls(cho, DcTerms.identifier);
			for(String url: identifiers) {
				if(url.contains("/purl.pt/")) {
					if (url.contains("media/cover")) {
						report.setThumbnailIfEmpty(url);
					} else if(url.contains("media/")) {
						report.setIsShownByIfEmpty(url);
					} else {
						report.setIsShownAtIfEmpty(url);
					}
				}else
					report.tests.add( ChoLinksUtil.testLink(url));
			}
		} else if(dglabUris.contains(report.dataProvider)) {
			ArrayList<String> identifiers=ChoLinksUtil.listUrls(cho, DcTerms.identifier);
			ArrayList<String> relations=ChoLinksUtil.listUrls(cho, DcTerms.relation);
			if(!identifiers.isEmpty())
				report.setIsShownAtIfEmpty(identifiers.get(0));
			if(!relations.isEmpty())
				report.setThumbnailIfEmpty(relations.get(0));
		} else		
			report.tests = ChoLinksUtil.testAllLinks(cho, DcTerms.identifier);
		return report;
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
