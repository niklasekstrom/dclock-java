package icsync;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.slf4j.Logger;

public class LoggingScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor {
	private final Logger logger;

	public LoggingScheduledThreadPoolExecutor(int corePoolSize, Logger logger) {
		super(corePoolSize);
		this.logger = logger;
	}

	protected void afterExecute(Runnable r, Throwable t) {
		super.afterExecute(r, t);

		if (t == null && r instanceof Future<?>) {
			try {
				((Future<?>) r).get();
			} catch (CancellationException ce) {
			} catch (ExecutionException ee) {
				logger.info("A task run by the executor threw an exception:", ee.getCause());
			} catch (InterruptedException ie) {
				Thread.currentThread().interrupt();
			}
		}
	}
}
