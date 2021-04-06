package rossio.oaipmh;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.oclc.oai.harvester2.verb.HarvesterVerb;
import org.oclc.oai.harvester2.verb.ListRecords;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

/**
 * Provides an Iterator interface to an OAI-PMH harvest
 * 
 * @author Nuno Freire (nfreire@gmail.com)
 * @since 21 de Fev de 2011
 */
public class OaipmhHarvest extends OaipmhHarvestBase implements Iterator<OaiPmhRecord> {
	
    /* Creates a new instance of this class.
     * 
     * @param baseURL
     *            Oai server base url
     * @param metadataPrefix
     *            oai metadata prefix to harvest
     * @param setSpec
     *            oai set to harvest
     * @throws HarvestException
     */
    public OaipmhHarvest(String baseURL, String metadataPrefix, String setSpec) throws HarvestException {
        this(baseURL, null, null, metadataPrefix, setSpec);
    }

    /**
     * Creates a new instance of this class.
     * 
     * @param baseURL
     *            Oai server base url
     * @param from
     * @param until
     * @param metadataPrefix
     *            oai metadata prefix to harvest
     * @param setSpec
     *            oai set to harvest
     * @throws HarvestException
     */
    public OaipmhHarvest(String baseURL, String from, String until, String metadataPrefix,
                         String setSpec) throws HarvestException {
        super(baseURL, from, until, metadataPrefix, setSpec);
    }

    /**
     * Creates a new instance of this class.
     * 
     * @param baseURL
     *            Oai server base url
     * @param resumptionToken
     */
    public OaipmhHarvest(String baseURL, String resumptionToken) {
        super(baseURL, resumptionToken);
    }

    public boolean hasNext() {
        if (maxRecordsToRetrieve > 0 && totalRetrievedRecords >= maxRecordsToRetrieve)
            return false;

        try {
            fetchNextIfEmpty();
            return !records.isEmpty();
        } catch (HarvestException e) {
            throw new RuntimeException("Could not get the next records!", e);
        }
    }

    public OaiPmhRecord next() {
        try {
            fetchNextIfEmpty();
            totalRetrievedRecords++;
            return records.poll();
        } catch (HarvestException e) {
            throw new RuntimeException("Could not get the next records!", e);
        }
    }

    public void remove() {
        throw new UnsupportedOperationException("Sorry, not implemented.");
    }

}
