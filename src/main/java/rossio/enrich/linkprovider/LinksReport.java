package rossio.enrich.linkprovider;

import java.util.ArrayList;

import org.apache.jena.rdf.model.Resource;

public class LinksReport {
	public ArrayList<LinkTest> tests=new ArrayList<LinkTest>();
	
	public Resource item;
	
	public String isShownAt;
	public String isShownBy;
	public String thumbnail;
	public ArrayList<String> hasView;
	
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
	
	public void setThumbnailIfEmpty(String thumbnail) {
		if(this.thumbnail==null)
			this.thumbnail = thumbnail;
//		else
		setIsShownByIfEmpty(thumbnail);
	}

//	public void setLinkWithErrorIfEmpty(String linkWithError) {
//		if(this.linkWithError==null)
//			this.linkWithError = linkWithError;
//	}
	
//	public boolean isComplete() {
//		return isShownAt!=null && thumbnail!=null;
//	}
	
}
