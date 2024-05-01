package rossio.ingest.preprocess;

import java.io.IOException;

public class CpdocEntrevistasPreprocessor extends CpdocPreprocessor {
	
	public CpdocEntrevistasPreprocessor() throws IOException {
		super(new CsvMapping("mapping-cpdoc-entrevistas.csv", true));
	}

}
