package rossio.ingest.datasets.dspace;

import java.util.HashMap;
import java.util.HashSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Statement;
import org.w3c.dom.Element;

import rossio.data.models.Dc;
import rossio.data.models.DcTerms;
import rossio.util.XmlUtil;

public class DcValue {
	private static HashSet<String> simpleDcElements=new HashSet() {{
		add("contributor");
		add("coverage");
		add("creator");
		add("date");
		add("description");
		add("format");
		add("identifier");
		add("language");
		add("publisher");
		add("relation");
		add("rights");
		add("source");
		add("subject");
		add("title");
		add("type");
	}};
	private static HashMap<String, String> dctermsElements=new HashMap<String, String>(){{
		put("abstract", "description");
		put("accessRights", "rights");
		//put("accrualMethod", "");
		//put("accrualPeriodicity", "");
		//put("accrualPolicy", "");
		put("alternative", "title");
		//put("audience", "");
		put("available", "date");
		put("bibliographicCitation", "identifier");
		put("conformsTo", "relation");
		put("created", "date");
		put("dateAccepted", "date");
		put("dateCopyrighted", "date");
		put("dateSubmitted", "date");
		//put("educationLevel", "");
		put("extent", "format");
		put("hasFormat", "relation");
		put("hasPart", "relation");
		put("hasVersion", "relation");
		//put("instructionalMethod", "");
		put("isFormatOf", "relation");
		put("isPartOf", "relation");
		put("isReferencedBy", "relation");
		put("isReplacedBy", "relation");
		put("isRequiredBy", "relation");
		put("issued", "date");
		put("isVersionOf", "relation");
		put("license", "rights");
		//put("mediator", "");
		put("medium", "format");
		put("modified", "date");
		//put("provenance", "");
		put("references", "relation");
		put("replaces", "relation");
		put("requires", "relation");
		//put("rightsHolder", "");
		put("spatial", "coverage");
		put("tableOfContents", "description");
		put("temporal", "coverage");
		put("valid", "date");
	}};
	
	String element;
	String qualifier;
	String value;
	String lang;
	
	public DcValue() {
	}
	
	public DcValue(Element xmlElement) throws IllegalArgumentException {
		if(simpleDcElements.contains(xmlElement.getLocalName())) {
			element=xmlElement.getLocalName();
			qualifier="none";
		} else if(dctermsElements.containsKey(xmlElement.getLocalName())) {
			element=dctermsElements.get(xmlElement.getLocalName());
			qualifier=xmlElement.getLocalName();
		} else {
			throw new IllegalArgumentException("DC element unknown:" + xmlElement.getLocalName());
		}
		if(!StringUtils.isEmpty(xmlElement.getAttribute("lang")) || !StringUtils.isEmpty(xmlElement.getAttribute("xml:lang"))) {
			lang=StringUtils.isEmpty(xmlElement.getAttribute("lang")) ? 
					xmlElement.getAttribute("lang") 
					: xmlElement.getAttribute("xml:lang");
		}
		value=XmlUtil.getText(xmlElement);	
	}
	
	public DcValue(Statement st) throws IllegalArgumentException {
		if(!st.getPredicate().getNameSpace().equals(DcTerms.NS) && !st.getPredicate().getNameSpace().equals(Dc.NS))
			throw new IllegalArgumentException("DC element unknown:" + st.getPredicate().getLocalName());
		if(simpleDcElements.contains(st.getPredicate().getLocalName())) {
			element=st.getPredicate().getLocalName();
			qualifier="none";
		} else if(dctermsElements.containsKey(st.getPredicate().getLocalName())) {
			element=dctermsElements.get(st.getPredicate().getLocalName());
			qualifier=st.getPredicate().getLocalName();
		} else {
			throw new IllegalArgumentException("DC element unknown:" + st.getPredicate().getLocalName());
		}
		if(!st.getObject().isLiteral())
			throw new IllegalArgumentException("DC element is a resource:" + st.getPredicate().getLocalName());
		 
		Literal lit=st.getObject().asLiteral();
		if(!StringUtils.isEmpty(lit.getLanguage())) 
			lang=lit.getLanguage();
		value=lit.getValue().toString();	
	}

	public static boolean isDcterms(Property property) {
		return simpleDcElements.contains(property.getLocalName())
			|| dctermsElements.containsKey(property.getLocalName());
	}
	
	
	public Element toDspaceDcElement(Element parentEl) {
		Element valEl = parentEl.getOwnerDocument().createElement("dcvalue");
		valEl.setAttribute("element", element); 
		if(StringUtils.isEmpty(qualifier))
			valEl.setAttribute("qualifier", qualifier);
		else
			valEl.setAttribute("qualifier", "none");
		if(!StringUtils.isEmpty(lang))
			valEl.setAttribute("language", lang); 
		if(!StringUtils.isEmpty(value))
			valEl.setTextContent(value);
		parentEl.appendChild(valEl);
		return valEl;
	}
	
	public String getElement() {
		return element;
	}
	public void setElement(String element) {
		this.element = element;
	}
	public String getQualifier() {
		return qualifier;
	}
	public void setQualifier(String qualifier) {
		this.qualifier = qualifier;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	public String getLang() {
		return lang;
	}
	public void setLang(String lang) {
		this.lang = lang;
	}	
}
