package eu.spice.uploaders.rdfuploader.model;

import java.util.Properties;

import org.apache.jena.rdf.model.Model;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.spice.rdfuploader.RDFUploaderContext;
import eu.spice.rdfuploader.uploaders.Utils;

public class JSONRequestUpdate implements Request {

	private JSONObject payload;
	private RDFUploaderContext context;
	private String datasetId, docId;
	private final static Logger logger = LoggerFactory.getLogger(JSONRequestUpdate.class);

	public JSONRequestUpdate(String datasetId, String docId, JSONObject payload, RDFUploaderContext context) {
		this.payload = payload;
		this.context = context;
		this.docId = docId;
		this.datasetId = datasetId;
	}

	public JSONObject getPayload() {
		return payload;
	}

	public void setPayload(JSONObject payload) {
		this.payload = payload;
	}

	@Override
	public void accomplishRequest() throws Exception {

		String root = context.getRootURI(datasetId, docId);
		String ontologyPrefix = context.getOntologyURIPrefix(datasetId, docId);
		String graphURI = context.getGraphURI(datasetId, docId);
		Properties namespaceProperties = Utils.loadProperties(context.getConf().getBlazegraphPropertiesFilepath());
		Model m = Utils.readOrTriplifyJSONObject(payload, root, ontologyPrefix);
		context.getBlazegraphClient().uploadModel(m, getTargetNamespace(), graphURI, namespaceProperties, true);
		logger.trace("Update Dataset Request - Accomplished");

	}

	@Override
	public String getDocId() {
		return null;
	}

	public String getTargetNamespace() {
		return context.getBlazegraphNamespace(datasetId);
	}

	public String getRepositoryURL() {
		return context.getBlazegraphClient().getRepositoryURL();
	}

}
