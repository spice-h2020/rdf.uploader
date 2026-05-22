package eu.spice.uploaders.rdfuploader.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.spice.rdfuploader.RDFUploaderContext;

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
		context.getTripleStoreClient().clearGraph(getTargetNamespace(),
				context.getGraphURI(datasetId, docIdClean));
		accomplished = true;
	}

	@Override
	public String getDocId() {
		return docId;
	}

	public String getTargetNamespace() {
		return context.getNamespace(datasetId);
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
