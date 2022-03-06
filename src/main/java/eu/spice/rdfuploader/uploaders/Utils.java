package eu.spice.rdfuploader.uploaders;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

import org.apache.commons.io.FilenameUtils;
import org.apache.jena.graph.Graph;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.sparqlanything.engine.FacadeX;
import com.github.sparqlanything.json.JSONTriplifier;
import com.github.sparqlanything.model.BaseFacadeXGraphBuilder;
import com.github.sparqlanything.model.FacadeXGraphBuilder;
import com.github.sparqlanything.model.IRIArgument;
import com.github.sparqlanything.model.Triplifier;
import com.github.sparqlanything.model.TriplifierHTTPException;

import eu.spice.rdfuploader.Constants.RDFJobsConstants;

public class Utils {

	private static final Logger logger = LoggerFactory.getLogger(Utils.class);

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

	public static Model readOrTriplifyJSONObject(JSONObject obj, String root, String ontologyURIPrefix)
			throws IOException, TriplifierHTTPException {

		JSONTriplifier jt = new JSONTriplifier();
		Properties p = new Properties();
		p.setProperty(IRIArgument.CONTENT.toString(), obj.toString());
		p.setProperty(IRIArgument.NAMESPACE.toString(), ontologyURIPrefix);
		if (root != null) {
			logger.trace("Setting root URI {}", root);
			p.setProperty(IRIArgument.BLANK_NODES.toString(), "false");
			p.setProperty(IRIArgument.ROOT.toString(), root);

		}

		Model m = ModelFactory.createDefaultModel();
		logger.trace("Reading as JSON-LD");
		RDFDataMgr.read(m, new StringReader(obj.toString()), "", Lang.JSONLD);
		logger.trace("Read " + m.size() + " triples from JSON-LD format!");
		if (m.size() == 0) {
			logger.trace("Trying to transform JSON document to RDF.");

			// Version 0.6.0
			FacadeXGraphBuilder builder = new BaseFacadeXGraphBuilder("uploader", p);
			jt.triplify(p, builder);
			Graph g = builder.getDatasetGraph().getDefaultGraph();
			m = ModelFactory.createModelForGraph(g);

//			m = ModelFactory
//					.createModelForGraph(jt.triplify(p, new BaseFacadeXBuilder("uploader", p)).getDefaultGraph());

			logger.trace("Read " + m.size() + " triples from JSON!");
		}

		return m;
	}

	public static Model triplifyFile(File file, String root, String ontologyURIPrefix) throws IOException,
			TriplifierHTTPException, InstantiationException, IllegalAccessException, IllegalArgumentException,
			InvocationTargetException, NoSuchMethodException, SecurityException, ClassNotFoundException {

		Properties p = new Properties();
		p.setProperty(IRIArgument.LOCATION.toString(), file.getAbsolutePath());
		p.setProperty(IRIArgument.NAMESPACE.toString(), ontologyURIPrefix);
		if (root != null) {
			p.setProperty(IRIArgument.BLANK_NODES.toString(), "false");
			p.setProperty(IRIArgument.ROOT.toString(), root);
		}

		logger.trace("Registry==null? {}", FacadeX.Registry == null);
		logger.trace("Registered extensions {}", FacadeX.Registry.getRegisteredExtensions());
		String triplifierClass = FacadeX.Registry
				.getTriplifierForExtension(FilenameUtils.getExtension(file.getAbsolutePath()));
		logger.trace("Triplifier class {} for file {}", triplifierClass, file.getAbsolutePath());
		Triplifier t = (Triplifier) Class.forName(triplifierClass).getConstructor().newInstance();

//		Graph g = t.triplify(p, new BaseFacadeXBuilder("uploader", p)).getDefaultGraph();

		// Version 0.6.0
		FacadeXGraphBuilder builder = new BaseFacadeXGraphBuilder("uploader", p);
		t.triplify(p, builder);
		Graph g = builder.getDatasetGraph().getDefaultGraph();

		logger.trace("{} triples generated from file {}", g.size(), file.getName());

		return ModelFactory.createModelForGraph(g);
	}

	public static File makeTempFolderForDataset(String tempFolder, String dataset) {
		File tempFolderDataset = new File(tempFolder + "/" + dataset);
		if (!tempFolderDataset.exists()) {
			tempFolderDataset.mkdirs();
		}
		return tempFolderDataset;
	}

}
