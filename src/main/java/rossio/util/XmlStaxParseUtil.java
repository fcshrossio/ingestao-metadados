package rossio.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;

import javax.xml.namespace.QName;
import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class XmlStaxParseUtil {
	public interface Handler {
		public boolean isRecordElement(XMLStreamReader reader);

		public boolean handleRecord(Document rec);
	}

	private XmlStaxParseUtil() {
	}

	public static void parse(File xmlFile, Handler handler) {
		new XmlStaxParseUtil().parseFile(xmlFile, handler);
	}

	private void parseFile(File xmlFile, Handler handler) {
		try {
			XMLInputFactory factory = XMLInputFactory.newInstance();
			XMLStreamReader reader = factory.createXMLStreamReader(new FileInputStream(xmlFile), "UTF8");
			while (reader.hasNext()) {
				int event = reader.next();
				if (event == XMLStreamConstants.START_ELEMENT && handler.isRecordElement(reader))
					handler.handleRecord(buildDom(reader));
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (FactoryConfigurationError e) {
			e.printStackTrace();
		} catch (XMLStreamException e) {
			e.printStackTrace();
		}
	}

	private Element toXmlElement(XMLStreamReader reader, Document ownerDoc) {
		Element el = ownerDoc.createElementNS(reader.getName().getNamespaceURI(), reader.getName().getLocalPart());
		for (int i = 0; i < reader.getAttributeCount(); i++) {
			QName attrName = reader.getAttributeName(i);
			el.setAttributeNS(attrName.getNamespaceURI(), attrName.getLocalPart(), reader.getAttributeValue(i));
		}
		return el;
	}

	private void buildElement(XMLStreamReader reader, Element el) throws DOMException, XMLStreamException {
		int event = -1;
		while (reader.hasNext() && event != XMLStreamConstants.END_ELEMENT) {
			event = reader.next();
			switch (event) {
			case XMLStreamConstants.START_ELEMENT:
				Element subEl = toXmlElement(reader, el.getOwnerDocument());
				el.appendChild(subEl);
				buildElement(reader, subEl);
				break;
			case XMLStreamConstants.CDATA: {
				String txt = reader.getText().trim();
				if (!StringUtils.isEmpty(txt))
					el.setTextContent(txt);
				break;
			}
			case XMLStreamConstants.CHARACTERS: {
				String txt = reader.getText().trim();
				if (!StringUtils.isEmpty(txt))
					el.setTextContent(txt);
				break;
			}
			case XMLStreamConstants.END_ELEMENT:
				break;
			case XMLStreamConstants.START_DOCUMENT:
				break;
			}
		}
	}

	private Document buildDom(XMLStreamReader reader) throws DOMException, XMLStreamException {
		Document doc = XmlUtil.newDocument();
		Element topEl = toXmlElement(reader, doc);
		doc.appendChild(topEl);
		buildElement(reader, topEl);
		return doc;
	}

	public static void main(String[] args) throws Exception {
		XmlStaxParseUtil.parse(new File("src/data/test/Cinemateca.xml"), new Handler() {
			public boolean isRecordElement(XMLStreamReader reader) {
				return reader.getLocalName().equals("Av");
			}

			public boolean handleRecord(Document rec) {
				return true;
			}
		});
	}
}
