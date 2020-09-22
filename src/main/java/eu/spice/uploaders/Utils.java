package eu.spice.uploaders;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

import org.openrdf.model.Statement;
import org.openrdf.query.GraphQueryResult;

import com.bigdata.rdf.sail.webapp.SD;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;

public class Utils {

	/*
	 * Check if a blazegraph namespace already exists.
	 */
	public static boolean namespaceExists(RemoteRepositoryManager repo, String namespace) throws Exception {

		GraphQueryResult res = repo.getRepositoryDescriptions();
		try {
			while (res.hasNext()) {
				Statement stmt = res.next();
				if (stmt.getPredicate().toString().equals(SD.KB_NAMESPACE.stringValue())) {
					if (namespace.equals(stmt.getObject().stringValue())) {
						return true;
					}
				}
			}
		} finally {
			res.close();
		}
		return false;
	}

	public static Properties loadProperties(String resource) throws IOException {
		Properties p = new Properties();
		InputStream is = new FileInputStream(new File(resource));
		p.load(new InputStreamReader(new BufferedInputStream(is)));
		return p;
	}

}
