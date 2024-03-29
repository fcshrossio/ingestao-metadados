package rossio.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.ls.DOMImplementationLS;
import org.w3c.dom.ls.LSOutput;
import org.w3c.dom.ls.LSSerializer;
import org.xml.sax.SAXException;

import net.sf.saxon.expr.InstanceOfExpression;



/**
 * Utility methods for working with XML DOMs (org.w3c.dom)
 * 
 * @author Nuno Freire (nfreire@gmail.com)
 * @since 23 de Fev de 2011
 */
public class XmlUtil {
	private static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(XmlUtil.class);
	
    /**
     * Schema factory
     */
    private static final SchemaFactory    W3X_XML_SCHEMA_FACTORY = SchemaFactory.newInstance(javax.xml.XMLConstants.W3C_XML_SCHEMA_NS_URI);

    /**
     * Document builder factory
     */
    private static DocumentBuilderFactory factory                = DocumentBuilderFactory.newInstance();
    private static TransformerFactory transFactory                = null;
    static {
    	System.setProperty("javax.xml.transform.TransformerFactory","net.sf.saxon.TransformerFactoryImpl");
    	transFactory = TransformerFactory.newInstance();    
    	
        factory.setNamespaceAware(true);
    }

    /**
     * Creates a new Document using the default XML implementation
     * 
     * @return DOM
     */
    public static Document newDocument() {
        try {
            return factory.newDocumentBuilder().newDocument();
        } catch (ParserConfigurationException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /**
     * An iterable for all the Element childs of a node
     * 
     * @param n
     *            the node get the children from
     * @return An iterable for all the Element childs of a node
     */
    public static Iterable<Element> elements(Element n) {
        int sz = n.getChildNodes().getLength();
        ArrayList<Element> elements = new ArrayList<Element>(sz);
        for (int idx = 0; idx < sz; idx++) {
            Node node = n.getChildNodes().item(idx);
            if (node instanceof Element) elements.add((Element)node);
        }
        return elements;
    }


	public static String getText(Node candidate) {
		String text="";
        NodeList childNodes = candidate.getChildNodes();
        for (int idx = 0; idx < childNodes.getLength(); idx++) {
            Node childNode = childNodes.item(idx);
            	String nTxt=childNode.getNodeValue();
            	if(nTxt!=null)
               text+=nTxt;
            
        }
        return text;
	}

	public static Element createResource(Element subjectElement, String propNamespace, String propName, String rdfResourceIri) {
		Element wrElSrvcProp = subjectElement.getOwnerDocument().createElementNS(propNamespace, propName);
		wrElSrvcProp.setAttributeNS(XmlNsUtil.RDF, "rdf:about", rdfResourceIri);
		subjectElement.appendChild(wrElSrvcProp);
		return wrElSrvcProp;
	}
	
	public static Element createPropertyWithReference(Element subjectElement, String propNamespace, String propName, String rdfResourceIri) {
		Element wrElSrvcProp = subjectElement.getOwnerDocument().createElementNS(propNamespace, propName);
		wrElSrvcProp.setAttributeNS(XmlNsUtil.RDF, "rdf:resource", rdfResourceIri);
		subjectElement.appendChild(wrElSrvcProp);
		return wrElSrvcProp;
	}
	
	public static Element createPropertyWithText(Element subjectElement, String propNamespace, String propName, String textValue) {
		Element wrElSrvcProp = subjectElement.getOwnerDocument().createElementNS(propNamespace, propName);
		wrElSrvcProp.setTextContent(textValue);
		subjectElement.appendChild(wrElSrvcProp);
		return wrElSrvcProp;
	}

	public static Element findResource(Document dom, String resourceUri) {
		try {
			return XPathUtil.queryDomForElement(XmlNsUtil.xpathEdmPrefixMap, "//*[@rdf:about=\""+resourceUri+"\"]", dom);
		} catch (XPathExpressionException e) {
			try {
				return XPathUtil.queryDomForElement(XmlNsUtil.xpathEdmPrefixMap, "//*[@rdf:about='"+resourceUri+"']", dom);
			} catch (XPathExpressionException e2) {
				throw new RuntimeException(e.getMessage() + "\n" + resourceUri, e);
			}
		}
	}

	public static Element removeResource(Document dom, String resourceUri) {
		Element el=findResource(dom, resourceUri);
		if(el==null)
			return null;
		return (Element) dom.getDocumentElement().removeChild(el);
	}
	
    /**
     * An Iterable for the Element's childs, with a particular name, of a node
     * 
     * @param n
     *            the node get the children from
     * @param elementName
     *            the name of the child elements
     * @return An Iterable for the Element's children, with a particular name, of a node
     */
    public static Iterable<Element> elements(Element n, String elementName) {
        NodeList subNodes = n.getElementsByTagName(elementName);
        int sz = subNodes.getLength();
        ArrayList<Element> elements = new ArrayList<Element>(sz);
        for (int idx = 0; idx < sz; idx++) {
            Node node = subNodes.item(idx);
            elements.add((Element)node);
        }
        return elements;
    }

    /**
     * An Iterable for the Element's childs, with a particular name, of a node
     * 
     * @param n
     *            the node get the children from
     * @param namespace
     * @param elementName
     *            the name of the child elements
     * @return An Iterable for the Element's children, with a particular name, of a node
     */
    public static Iterable<Element> elements(Element n, String namespace, String elementName) {
        NodeList subNodes = n.getElementsByTagNameNS(namespace, elementName);
        int sz = subNodes.getLength();
        ArrayList<Element> elements = new ArrayList<Element>(sz);
        for (int idx = 0; idx < sz; idx++) {
            Node node = subNodes.item(idx);
            elements.add((Element)node);
        }
        return elements;
    }

    /**
     * Gets the first child Element with a given name
     * 
     * @param n
     *            the node get the children from
     * @param elementName
     *            the name of the child elements
     * @return the first child Element with a given name
     */
    public static Element getElementByTagNameIgnoreCase(Element n, String elementName) {
        NodeList subNodes = n.getChildNodes();
        int sz = subNodes.getLength();
        for (int idx = 0; idx < sz; idx++) {
            Node node = subNodes.item(idx);
            if(node instanceof Element && ((Element)node).getTagName().equalsIgnoreCase(elementName))
            	return(Element)node;
        }
        return null;
    }

    public static String getElementTextByTagNameIgnoreCase(Element n, String elementName) {
        Element subEl = getElementByTagNameIgnoreCase(n, elementName);
        if (subEl!=null) return getElementText(subEl);
        return null;
    }

    public static String getElementText(Element n) {
        NodeList childNodes = n.getChildNodes();
        String result = new String();
        for (int i = 0; i < childNodes.getLength(); i++) {
          Node node = childNodes.item(i);
          if (node.getNodeType() == Node.TEXT_NODE) {
            result += node.getNodeValue();
          }
        }
        return result;
    }

    /**
     * Gets the first child Element with a given name
     * 
     * @param n
     *            the node get the children from
     * @param namespace
     *            the namespace of the child element
     * @param elementName
     *            the name of the child elements
     * @return the first child Element with a given name
     */
    public static Element getElementByQualifiedName(Element n, String namespace, String elementName) {
        NodeList subNodes = n.getElementsByTagNameNS(namespace, elementName);
        int sz = subNodes.getLength();
        if (sz > 0) return (Element)subNodes.item(0);
        return null;
    }

    /**
     * Creates a DOM from a file representation of an xml record
     * 
     * @param doc
     *            the xml file
     * @return the DOM document
     */
    public static Document parseDomFromFile(File doc) {
    	FileInputStream reader;
        try {
            reader = new FileInputStream(doc);
//            reader = new FileReader(doc);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Could not open file '" + doc.getName() + "'!", e);
        }
        return parseDom(reader);
    }

    /**
     * Creates a DOM from a file representation of an xml record
     * 
     * @param reader
     *            the xml reader
     * @return the DOM document
     */
    public static Document parseDom(Reader reader) {
        try {
            DocumentBuilder builder = factory.newDocumentBuilder();
            return builder.parse(new org.xml.sax.InputSource(reader));
        } catch (Exception e) {
            throw new RuntimeException("Could not parse DOM for '" + reader.toString() + "'!", e);
        }
    }
    public static Document parseDom(InputStream reader) {
    	try {
    		DocumentBuilder builder = factory.newDocumentBuilder();
    		return builder.parse(new org.xml.sax.InputSource(reader));
    	} catch (Exception e) {
    		throw new RuntimeException("Could not parse DOM for '" + reader.toString() + "'!", e);
    	}
    }

    /**
     * @param xml
     * @return pretty xml
     */
    public static String prettyXml(String xml) {
        TransformerFactory tfactory = TransformerFactory.newInstance();
        Transformer serializer;
        try {
            Source source = new StreamSource(new StringReader(xml));
            StringWriter writer = new StringWriter();

            serializer = tfactory.newTransformer();
            // Setup indenting to "pretty print"
            serializer.setOutputProperty(OutputKeys.INDENT, "yes");
            serializer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");

            serializer.transform(source, new StreamResult(writer));
            return writer.toString();
        } catch (Exception e) {
            return xml;
        }
    }

    /**
     * @param dom
     * @param outFile
     */
    public static void writeDomToFile(Document dom, File outFile) {
        try {
            TransformerFactory transFact = TransformerFactory.newInstance();
            Transformer transformer = transFact.newTransformer();
            DOMSource source = new DOMSource(dom);
            StreamResult result;

            result = new StreamResult(new OutputStreamWriter(new FileOutputStream(outFile),
                    System.getProperty("file.encoding")));
            transformer.transform(source, result);
        } catch (Exception e) {
            throw new RuntimeException("Could not write dom tree '" + dom.getBaseURI() +
                                       "' to file '" + outFile.getName() + "'!", e);
        }
    }

    /**
     * Converts a dom to a String
     * 
     * @param dom
     *            dom to convert
     * @return the dom as a String
     */
    public static String writeDomToString(Document dom) {
        return writeDomToString(dom, null);
    }

    /**
     * Converts a dom to a String
     * 
     * @param dom
     *            dom to convert
     * @param outputProperties
     *            the properties for the String representation of the XML
     * @return the dom as a String
     */
    public static String writeDomToString(Element dom, Properties outputProperties) {
        try {
            StringWriter ret = new StringWriter();
            TransformerFactory transFact = TransformerFactory.newInstance();
// transFact.setAttribute("indent-number", 2);
            Transformer transformer = transFact.newTransformer();
            if (outputProperties != null) transformer.setOutputProperties(outputProperties);
            DOMSource source = new DOMSource(dom);
            StreamResult result = new StreamResult(ret);
            transformer.transform(source, result);
            return ret.toString();
        } catch (Exception e) {
            throw new RuntimeException("Could not write dom to string!", e);
        }
    }

    /**
     * Converts a dom to a String
     * 
     * @param dom
     *            dom to convert
     * @param outputProperties
     *            the properties for the String representation of the XML
     * @return the dom as a String
     */
    public static String writeDomToString(Document dom, Properties outputProperties) {
        try {
            StringWriter ret = new StringWriter();
            TransformerFactory transFact = TransformerFactory.newInstance();
// transFact.setAttribute("indent-number", 2);
            Transformer transformer = transFact.newTransformer();
            if (outputProperties != null) transformer.setOutputProperties(outputProperties);
            DOMSource source = new DOMSource(dom);
            StreamResult result = new StreamResult(ret);
            transformer.transform(source, result);
            return ret.toString();
        } catch (Exception e) {
            throw new RuntimeException("Could not write dom to string!", e);
        }
    }

    /**
     * Converts a dom element to a String
     * 
     * @param node
     * @return the dom as a String
     */
    public static String writeDomToString(Element node) {
        DOMImplementation domImplementation = node.getOwnerDocument().getImplementation();
        if (domImplementation.hasFeature("LS", "3.0") &&
            domImplementation.hasFeature("Core", "2.0")) {
            DOMImplementationLS domImplementationLS = (DOMImplementationLS)domImplementation.getFeature(
                    "LS", "3.0");
            LSSerializer lsSerializer = domImplementationLS.createLSSerializer();

            LSOutput lsOutput = domImplementationLS.createLSOutput();
            lsOutput.setEncoding("UTF-8");

            StringWriter stringWriter = new StringWriter();
            lsOutput.setCharacterStream(stringWriter);
            lsSerializer.write(node, lsOutput);
            return stringWriter.toString();
        } else {
            throw new RuntimeException("DOM 3.0 LS and/or DOM 2.0 Core not supported.");
        }
    }

    /**
     * Creates a new Schema using the default XML implementation
     * 
     * @param xmlSchema
     * @return Schema
     */
    public static Schema newSchema(File xmlSchema) {
        try {
            return W3X_XML_SCHEMA_FACTORY.newSchema(xmlSchema);
        } catch (SAXException e) {
            throw new RuntimeException("Could not create schema for file '" + xmlSchema.getName() +
                                       "'!", e);
        }
    }

    /**
     * Creates a new Schema using the default XML implementation
     * 
     * @param xmlSchema
     * @return Schema
     */
    public static Schema newSchema(Source xmlSchema) {
        try {
            return W3X_XML_SCHEMA_FACTORY.newSchema(xmlSchema);
        } catch (SAXException e) {
            throw new RuntimeException("Could not create schema for file '" + xmlSchema + "'!", e);
        }
    }

    /**
     * Validates XML on a XMLschema
     * 
     * @param xmlSchema
     * @param sourceXml
     */
    public static void validateXmlOnSchema(File xmlSchema, Source sourceXml) {
        Schema schema = newSchema(xmlSchema);
        try {
            schema.newValidator().validate(sourceXml);
        } catch (Exception e) {
            throw new RuntimeException("Could not validate '" + sourceXml.getSystemId() +
                                       "' with '" + xmlSchema.getName() + "'!", e);
        }
    }

    /**
     * Validates XML on a XMLschema
     * 
     * @param xmlSchema
     * @param sourceXml
     */
    public static void validateXmlOnSchema(Source xmlSchema, Source sourceXml) {
        Schema schema = newSchema(xmlSchema);
        try {
            schema.newValidator().validate(sourceXml);
        } catch (Exception e) {
            throw new RuntimeException("Could not validate '" + sourceXml.getSystemId() +
                                       "' with '" + xmlSchema.getSystemId() + "'!", e);
        }
    }

    /**
     * Extracts the encoding used in the xml string.
     * 
     * @param xml
     * @return encoding
     */
    public static String extractXmlEncoding(String xml) {
        String encoding = null;// "UTF-8";
        String prefix = "<?xml version=\"1.0\" encoding=\"";
        if (xml.substring(0, 200).contains(prefix)) {
            int start = xml.indexOf(prefix) + prefix.length();
            int end = xml.indexOf("\"", start);
            encoding = xml.substring(start, end);
        }
        return encoding;
    }

	public static void writeDomToStream(Document dom, OutputStreamWriter outputStream) {

        try {
            TransformerFactory transFact = TransformerFactory.newInstance();
            Transformer transformer = transFact.newTransformer();
            DOMSource source = new DOMSource(dom);
            StreamResult result;

            result = new StreamResult(outputStream);
            transformer.transform(source, result);
        } catch (Exception e) {
            throw new RuntimeException("Could not write dom tree '" + dom.getBaseURI() +
                                       "' to stream!", e);
        }
		
	}


    public static Document transform(Document dom, Document xslt) {
		return transform(dom, xslt, null);
    }

    public static Document transform(Document dom, Document xslt, Map<String, Object> parameters) {
    	try {
    		DOMSource xsltSource = new DOMSource(xslt);
    		Transformer transformer = transFactory.newTransformer(xsltSource);
    		DOMSource source = new DOMSource(dom);
    		Document out=newDocument();
    		DOMResult result = new DOMResult(out);
    		if(parameters!=null)
    			for(String key: parameters.keySet()) 
    				transformer.setParameter(key, parameters.get(key));
    		transformer.transform(source, result);
    		return out;
    	} catch (Exception e) {
    		throw new RuntimeException("Could not write dom tree '" + dom.getBaseURI() + "'!", e);
    	}
    }

	public static URI getElementUriFromRdfResourceOrText(Element element) {
		String url = (element.getAttributeNS("http://www.w3.org/1999/02/22-rdf-syntax-ns#", "resource"));
		if(StringUtils.isEmpty(url))
			url = XmlUtil.getElementText(element).trim();
		if(StringUtils.isEmpty(url))
			return null;
		try {
			return URI.create(url);
		} catch (Exception e) {
			log.debug("unparsable URI "+url, e);
			return null;
		}
	}
}
