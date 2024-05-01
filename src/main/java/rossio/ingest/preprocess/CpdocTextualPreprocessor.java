package rossio.ingest.preprocess;

import java.io.IOException;

public class CpdocTextualPreprocessor extends CpdocPreprocessor {
	
	public CpdocTextualPreprocessor() throws IOException {
		super(new CsvMapping("mapping-cpdoc-textual.csv", true));
	}

}
