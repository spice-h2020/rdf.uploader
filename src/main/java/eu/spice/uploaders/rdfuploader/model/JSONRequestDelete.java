package eu.spice.uploaders.rdfuploader.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.spice.rdfuploader.RDFUploaderContext;
import eu.spice.rdfuploader.uploaders.Utils;

public class JSONRequestDelete implements Request {

	private static final Logger logger = LoggerFactory.getLogger(JSONRequestDelete.class);
	private String datasetId, docId;
	private RDFUploaderContext context;
	private boolean accomplished = false;

	public JSONRequestDelete(String datasetId, String docId, RDFUploaderContext context) {
		this.datasetId = datasetId;
		this.docId = docId;
		this.context = context;
	}

	public void accomplishRequest() throws Exception {
		logger.debug("CLEAR graph " + context.getGraphURI(datasetId, docId));
		String docIdClean = basicEscaper.escape(docId);
		context.getBlazegraphClient().clearGraph(getTargetNamespace(),
				Utils.loadProperties(context.getConf().getBlazegraphPropertiesFilepath()),
				context.getGraphURI(datasetId, docIdClean));
		accomplished = true;
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

	@Override
	public boolean isAccomplished() {
		// TODO Auto-generated method stub
		return accomplished;
	}

	@Override
	public String getDataset() {
		return datasetId;
	}
	
	

}
