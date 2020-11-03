package eu.spice.rdfuploader;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import eu.spice.rdfuploader.uploaders.Uploader;
import eu.spice.uploaders.rdfuploader.model.Request;

public class RDFUploader {
	
	private static final Logger logger = LogManager.getLogger(RDFUploader.class);

	public static void main(String[] args) throws IOException, InterruptedException {
		logger.info("SPICE RDF Publisher");
		RDFUploaderConfiguration c = RDFUploaderConfiguration.getInstance();
		BlockingQueue<Request> requests = new LinkedBlockingQueue<>(c.getRequestQueueSize());
		String tmpFolder = c.getTmpFolder();
		new File(tmpFolder).mkdir();
		Uploader up = new Uploader(requests, tmpFolder);

		logger.info("The activity log watchdog will start in 60 secs.");
		Thread t = new Thread(up);
		t.start();
		ScheduledExecutorService ses = Executors.newScheduledThreadPool(1);
		ses.scheduleAtFixedRate(new ActivityLogWatchdog(c, requests), 60, c.getLookupRateSeconds(), TimeUnit.SECONDS);

//		up.stop();
		t.join();

	}

}
