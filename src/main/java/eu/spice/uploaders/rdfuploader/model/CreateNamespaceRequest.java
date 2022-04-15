package eu.spice.uploaders.rdfuploader.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;

import eu.spice.rdfuploader.RDFUploaderContext;
import eu.spice.rdfuploader.uploaders.Utils;

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

	public String getRepositoryURL() {
		return context.getBlazegraphClient().getRepositoryURL();
	}

	public void accomplishRequest() throws Exception {
		logger.debug("Create Namespace Request");
		RemoteRepositoryManager manager = new RemoteRepositoryManager(this.getRepositoryURL());
		this.context.getBlazegraphClient().createAndGetRemoteRepositoryForNamespace(manager, getTargetNamespace(),
				Utils.loadProperties(context.getConf().getBlazegraphPropertiesFilepath()));
		manager.close();
		logger.trace("Create Namespace Request.. accomplished");
		accomplished = true;
	}

	@Override
	public String getDocId() {
		return null;
	}

	public String getTargetNamespace() {
		return context.getBlazegraphNamespace(datasetId);
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
