package rossio.ingest.solr.manager;

import java.text.DecimalFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class ProgressLogger {
	
	Logger log;
	Integer countInterval;
	Duration timeInterval;
	String message;
	
	int lastLogCount;
	LocalDateTime lastLogTime;
	LocalDateTime firstLogTime;
	
	int cnt;
	
	public ProgressLogger(Logger log, String message, Integer countInterval, Duration timeInterval) {
		super();
		this.log = log;
		this.message = message;
		this.countInterval = countInterval;
		this.timeInterval = timeInterval;
		firstLogTime=LocalDateTime.now();
		lastLogTime=firstLogTime;
	}
	
	public void log() {
		cnt++;
		if(countInterval!=null && cnt==lastLogCount+countInterval)
			printLog();
		else if(timeInterval!=null && lastLogTime.isBefore(LocalDateTime.now().minus(timeInterval)))
			printLog();
	}

	private void printLog() {
		double ops=cnt-lastLogCount;
		
		LocalDateTime now = LocalDateTime.now();
		Duration span = Duration.between(lastLogTime, now);
		
		double opsPerSec=ops / span.getSeconds();
		DecimalFormat decFmt=new DecimalFormat("###.#"); 
		
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm");
		String nowStr = now.format(formatter);
		if(opsPerSec>60)
			log.log(nowStr+"-"+message+": "+cnt+ "("+decFmt.format(opsPerSec)+"/sec)");		
		else {
			opsPerSec=opsPerSec * 60;
			log.log(nowStr+"-"+message+": "+cnt+ "("+decFmt.format(opsPerSec)+"/min)");		
		}	
		lastLogCount=cnt;
		lastLogTime=now;
	}
	
	
	
	
	
	public static void main(String[] args) throws Exception {
		Logger log=new Logger("target/log-test.txt");
		 ProgressLogger plog=new ProgressLogger(log, "Test log", 50, Duration.ofSeconds(30));
		 
		 for (int i=0; i<1000; i++) {
			 plog.log();
			 Thread.sleep(900);
		 }
		
	}
	
	
	
}
