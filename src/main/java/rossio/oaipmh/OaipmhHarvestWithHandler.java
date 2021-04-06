package rossio.oaipmh;

/**
 * Provides an Iterator interface to an OAI-PMH harvest
 * 
 * @author Nuno Freire (nfreire@gmail.com)
 * @since 21 de Fev de 2011
 */
public class OaipmhHarvestWithHandler extends OaipmhHarvestBase {
	
	public interface Handler extends OaipmhHarvestBase.WarnHandler {
		public boolean handle(OaiPmhRecord record);
	}
	
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
    public OaipmhHarvestWithHandler(String baseURL, String metadataPrefix, String setSpec) throws HarvestException {
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
    public OaipmhHarvestWithHandler(String baseURL, String from, String until, String metadataPrefix,
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
    public OaipmhHarvestWithHandler(String baseURL, String resumptionToken) {
        super(baseURL, resumptionToken);
    }

    public void run(Handler handler) {
    	setWarnHandler(handler);
    	for(OaiPmhRecord rec=next(); rec!=null; rec=next()) {
    		if (!handler.handle(rec))
    			break;
    	}
    }

    protected OaiPmhRecord next() {
        try {
            fetchNextIfEmpty();
            totalRetrievedRecords++;
            return records.poll();
        } catch (HarvestException e) {
            throw new RuntimeException("Could not get the next records!", e);
        }
    }
    
}
