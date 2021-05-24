package rossio.ingest.solr.manager;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import org.apache.commons.io.FileUtils;

public class StopFile {
	public interface StopFileListener{
		public void signalStop();
	}
	
	File stopFile;
	ArrayList<StopFileListener> listeners=new ArrayList<StopFile.StopFileListener>();
	
	public StopFile(File stopFile) {
		super();
		this.stopFile = stopFile;
	}

	public boolean checkStop() {
		String content;
		try {
			if(!stopFile.exists())
				return false;
			content = FileUtils.readFileToString(stopFile, StandardCharsets.UTF_8);
			if( content.toLowerCase().contains("stop") ) {
				for(StopFileListener listener : listeners)
					listener.signalStop();
				return true;
			}
		} catch (IOException e) {
			e.printStackTrace();
			//ignore or debug...
		}
		return false;
	}
	
	public void addListener(StopFileListener l) {
		listeners.add(l);
	}

	public void waitUntilStop() throws InterruptedException {
		while (!checkStop())
			Thread.sleep(5000);
	}
}
