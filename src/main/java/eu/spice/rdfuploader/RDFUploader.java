package eu.spice.rdfuploader;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import eu.spice.rdfuploader.uploaders.Uploader;
import eu.spice.uploaders.rdfuploader.model.Request;

public class RDFUploader {

	public static void main(String[] args) throws IOException, InterruptedException {
		RDFUploaderConfiguration c = RDFUploaderConfiguration.getInstance();
		BlockingQueue<Request> requests = new LinkedBlockingQueue<>(c.getRequestQueueSize());
		String tmpFolder = c.getTmpFolder();
		new File(tmpFolder).mkdir();
		Uploader up = new Uploader(requests, tmpFolder);

		Thread t = new Thread(up);
		t.start();

		ScheduledExecutorService ses = Executors.newScheduledThreadPool(1);
		ses.scheduleAtFixedRate(new ActivityLogWatchdog(c, requests), 0, c.getLookupRateSeconds(), TimeUnit.SECONDS);

//		up.stop();
		t.join();

	}

}
