package eu.spice.uploaders.rdfuploader.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.spice.rdfuploader.RDFUploaderContext;

public class CreateNamespaceRequest implements Request {

	private String datasetId;
	private RDFUploaderContext context;
	private final static Logger logger = LoggerFactory.getLogger(CreateNamespaceRequest.class);
	private boolean accomplished=false;

	public CreateNamespaceRequest(String datasetId, RDFUploaderContext context) {
		super();
		this.datasetId = datasetId;
		this.context = context;
	}

	public void accomplishRequest() throws Exception {
		logger.debug("Create Namespace Request");
		this.context.getTripleStoreClient().createNamespace(getTargetNamespace());
		logger.trace("Create Namespace Request.. accomplished");
		accomplished = true;
	}

	@Override
	public String getDocId() {
		return null;
	}

	public String getTargetNamespace() {
		return context.getNamespace(datasetId);
	}

	@Override
	public boolean isAccomplished() {
		return accomplished;
	}

	@Override
	public String getDataset() {
		return datasetId;
	}

}
