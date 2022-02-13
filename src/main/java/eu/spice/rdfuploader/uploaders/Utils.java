package eu.spice.rdfuploader.uploaders;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

import org.json.JSONArray;
import org.json.JSONObject;

import eu.spice.rdfuploader.Constants.RDFJobsConstants;

public class Utils {


	public static Properties loadProperties(String resource) throws IOException {
		Properties p = new Properties();
		InputStream is = new FileInputStream(new File(resource));
		p.load(new InputStreamReader(new BufferedInputStream(is)));
		return p;
	}

	public static void addMessage(JSONObject obj, String messageString) {
		JSONArray history;
		if (obj.has(RDFJobsConstants.HISTORY)) {
			history = obj.getJSONArray(RDFJobsConstants.HISTORY);
		} else {
			history = new JSONArray();
		}

		JSONObject message = new JSONObject();
		message.put(RDFJobsConstants.MESSAGE, messageString);
		message.put(RDFJobsConstants.TIMESTAMP, System.currentTimeMillis());
		history.put(message);
		obj.put(RDFJobsConstants.HISTORY, history);
	}

}
