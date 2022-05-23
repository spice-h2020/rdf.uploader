package eu.spice.rdfuploader.uploaders;

import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.spice.uploaders.rdfuploader.model.Request;

public class Uploader implements Runnable {

	private BlockingQueue<Request> requests;
	private boolean stop = false;
	private static final Logger logger = LoggerFactory.getLogger(Uploader.class);

	public Uploader(BlockingQueue<Request> uploadRequests, String tmpFolder) {
		super();
		this.requests = uploadRequests;
	}

	public void run() {
		logger.trace("Start uploader");
		while (!stop || !this.requests.isEmpty()) {
			try {
				logger.trace("Waiting for requests..");
				Request request = requests.take();
				logger.info("Accomplishing request ({}) on dataset {} docid {}", request.getClass().toString(),
						request.getDataset(), request.getDocId());
				logger.trace("Got request");
				request.accomplishRequest();
				logger.trace("Request accomplished");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		logger.trace("Stopping uploader");
	}

	public void stop() {
		stop = true;
	}

}
