package rossio.oaipmh;

public class OaiWrappedException extends Exception {
	byte[] responseBytes;
	
	public OaiWrappedException(byte[] responseBytes, Exception wrappedException) {
		super(wrappedException);
		this.responseBytes = responseBytes;
	}

	public byte[] getResponseBytes() {
		return responseBytes;
	}
}
