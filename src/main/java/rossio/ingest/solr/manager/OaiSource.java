package rossio.ingest.solr.manager;

import org.apache.commons.lang3.StringUtils;

public class OaiSource{
	public String dataProvider;
	public String baseUrl;
	public String set;
	public String metadataPrefix;
	public String status="";
	public String statusIndexing="";
	public String resumptionToken="";

	public OaiSource(String unparsed) {
		String[] split = unparsed.split("\\|");
		dataProvider=split[0];
		baseUrl=split[1];
		set=split[2];
		metadataPrefix=split[3];
		if(split.length>4)
			status=split[4];
		if(split.length>5)
			statusIndexing=split[5];
		if(split.length>6)
			resumptionToken=split[6];
		if(StringUtils.isEmpty(set))
			set=null;
		if(StringUtils.isEmpty(metadataPrefix))
			metadataPrefix="oai_dc";
		if(StringUtils.isEmpty(resumptionToken))
			resumptionToken=null;
	}
	
	public String getSourceId() {
		return baseUrl+"#"+(set==null ? "" : set);
	}

	public void updateStatus(String result) {
		if (result.startsWith("SUCCESS"))
			status="SUCCESS";
		else
			status="FAILURE";
	}

	public void updateStatusIndexing(String result) {
		if (result.startsWith("SUCCESS"))
			statusIndexing="SUCCESS";
		else
			statusIndexing="FAILURE";
	}

	public String serializeToString() {
		return dataProvider+"|"+baseUrl+"|"+(set==null ? "" : set)+"|"
			+(metadataPrefix==null ? "" : metadataPrefix)+"|"
			+(status==null ? "" : status)+"|"
			+(statusIndexing==null ? "" : statusIndexing)+"|"
			+(resumptionToken==null ? "" : resumptionToken);
	}
	

	public void updateSatus(String result) {
		if (result.startsWith("SUCCESS"))
			status="SUCCESS";
		else
			status="FAILURE";
	}

}