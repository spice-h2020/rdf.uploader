package eu.spice;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.json.JSONObject;

import eu.spice.uploaders.Uploader;
import eu.spice.uploaders.Utils;
import eu.spice.uploaders.model.JSONRequest;
import eu.spice.uploaders.model.Request;

public class App {

	public static void main(String[] args) throws IOException, InterruptedException {
		BlockingQueue<Request> requests = new LinkedBlockingQueue<>();
		String tmpFolder = "/Users/lgu/Desktop/tmp";
		new File(tmpFolder).mkdir();
		Uploader up = new Uploader(requests, tmpFolder);
		Properties p = Utils.loadProperties("src/main/resources/blazegraph.properties");

		JSONObject obj = new JSONObject("{'a':'b'}");
		JSONRequest jr = new JSONRequest("test4", "http://localhost:9999/blazegraph", null, p,
				obj, "https://w3id.org/spice/ontology/");
		jr.setResourceURIPrefix("https://w3id.org/resource/");
		Thread t = new Thread(up);
		t.start();

		requests.put(jr);
		up.stop();
		t.join();

	}

}
