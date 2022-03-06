package eu.spice.uploaders.rdfuploader.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.spice.rdfuploader.RDFUploaderContext;
import eu.spice.rdfuploader.uploaders.Utils;

public class JSONRequestDelete implements Request {

	private static final Logger logger = LoggerFactory.getLogger(JSONRequestDelete.class);
	private String datasetId, docId;
	private RDFUploaderContext context;

	public JSONRequestDelete(String datasetId, String docId, RDFUploaderContext context) {
		this.datasetId = datasetId;
		this.docId = docId;
		this.context = context;
	}

	public void accomplishRequest() throws Exception {
		logger.debug("CLEAR graph " + context.getGraphURI(datasetId, docId));
		context.getBlazegraphClient().clearGraph(getTargetNamespace(),
				Utils.loadProperties(context.getConf().getBlazegraphPropertiesFilepath()),
				context.getGraphURI(datasetId, docId));
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
