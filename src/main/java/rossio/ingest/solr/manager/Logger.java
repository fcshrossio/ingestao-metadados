package rossio.ingest.solr.manager;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

public class Logger{
	File logFile;
	SimpleDateFormat timeFormat=new SimpleDateFormat("yyyy-MM-dd HH:mm");
	
	public Logger(String logFilePath) {
		this.logFile = new File(logFilePath);
	}

	public void log(String message) {
		try {
			if(logFile!=null)
				FileUtils.write(logFile, timeFormat.format(new Date())+"| "+message+"\n", StandardCharsets.UTF_8, true);
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	public void log(String title, Exception e) {
		log(title+"\n"+ ExceptionUtils.getStackTrace(e));
	}
	
}