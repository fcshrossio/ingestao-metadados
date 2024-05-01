package rossio.ingest.preprocess;

import java.io.IOException;

public class CpdocAudiovisualPreprocessor extends CpdocPreprocessor {
	
	public CpdocAudiovisualPreprocessor() throws IOException {
		super(new CsvMapping("mapping-cpdoc-audiovisual.csv", true));
	}

}
