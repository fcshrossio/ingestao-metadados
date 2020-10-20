package rossio.ingestao.dspace;

import java.util.HashMap;
import java.util.HashSet;

import org.w3c.dom.Element;

import rossio.util.XmlUtil;

public class OaiDcToDspaceDcConverter {

	
	public static DcMetadata convert(Element metadata) {
		DcMetadata dspaceMd=new DcMetadata();
		for (Element el: XmlUtil.elements(metadata)) {
			dspaceMd.addValue(el);
		}
		return dspaceMd;
	}
}
