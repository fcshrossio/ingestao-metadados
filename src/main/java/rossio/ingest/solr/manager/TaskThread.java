package rossio.ingest.solr.manager;

public class TaskThread {
	public interface Task {
		public void run() throws Exception;

		public String getTitle();
	}

	Thread thread;
	boolean finished=false;
	Exception error;
	Task task;
	
	public TaskThread(Task task, Logger log) {
		super();
		this.task = task;
		thread=new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					task.run();
				} catch (Exception e) {
					error=e;
					log.log(task.getTitle(), e);
				}
				log.log("Task finished - "+task.getTitle());
				finished=true;
			}
		});
		thread.start();
	}

	public boolean isFinished() {
		return finished;
	}

	public Exception getError() {
		return error;
	}
	
	
	
	
}
