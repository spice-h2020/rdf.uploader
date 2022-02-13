package eu.spice.rdfuploader;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.spice.rdfuploader.uploaders.Uploader;
import eu.spice.uploaders.rdfuploader.model.Request;

public class RDFUploader {

	private static final Logger logger = LoggerFactory.getLogger(RDFUploader.class);

	public static void main(String[] args) throws IOException, InterruptedException {
		logger.info("SPICE RDF Publisher");
		RDFUploaderConfiguration c;
		if (args.length > 0) {
			c = RDFUploaderConfiguration.getInstance(args[0]);
		} else {
			c = RDFUploaderConfiguration.getInstance();
		}
		BlockingQueue<Request> requests = new LinkedBlockingQueue<>(c.getRequestQueueSize());

		File tmpFolder = new File(c.getTmpFolder());
		if (c.isClean() && tmpFolder.exists()) {
			logger.info("Cleaning tmp folder");
			FileUtils.deleteDirectory(tmpFolder);
		}

		tmpFolder.mkdir();

		Uploader up = new Uploader(requests, tmpFolder.getAbsolutePath());

		logger.info("The activity log watchdog will start in {} secs.", c.getInitalDealy());
		Thread t = new Thread(up);
		t.start();
		ScheduledExecutorService ses = Executors.newScheduledThreadPool(1);
		ses.scheduleAtFixedRate(new ActivityLogWatchdog(
				new RDFUploaderContext(c), requests), c.getInitalDealy(), c.getLookupRateSeconds(),
				TimeUnit.SECONDS);
		t.join();

	}

}
