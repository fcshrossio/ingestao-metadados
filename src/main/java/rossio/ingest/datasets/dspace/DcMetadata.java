package rossio.ingest.datasets.dspace;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.jena.rdf.model.Statement;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import rossio.util.XmlUtil;

public class DcMetadata {	
	
	ArrayList<DcValue> values=new ArrayList<DcValue>();

	public DcMetadata() {		
	}

	public DcMetadata(File inputFile) {		
		
	}

	public List<DcValue> getValues() {
		return values;
	}
	
	public String getAsXmlString() {
		Document dom = XmlUtil.newDocument();
		Element dcEl = dom.createElement("dublin_core");
		dom.appendChild(dcEl);
		for (DcValue v: values) {
			v.toDspaceDcElement(dcEl);
		}
		return XmlUtil.writeDomToString(dom);
	}

	public void addValue(Element el) {
		try {
			DcValue v=new DcValue(el);
			values.add(v);
		} catch (IllegalArgumentException e) {
			System.out.print("DEV WARN: ");
			e.printStackTrace(System.out);
		}
	}

	public void addValue(Statement st) {
		if(DcValue.isDcterms(st.getPredicate()) &&
				st.getObject().isLiteral()) {
			DcValue v=new DcValue(st);
			values.add(v);
		}
	}

	
	
}
