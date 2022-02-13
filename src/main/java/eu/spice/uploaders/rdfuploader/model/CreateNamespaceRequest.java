package eu.spice.uploaders.rdfuploader.model;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;

import eu.spice.rdfuploader.RDFUploaderContext;

public class CreateNamespaceRequest implements Request {

	private String namespace, repositoryURL;
	private Properties namespaceProperties;
	private RDFUploaderContext context;
	private final static Logger logger = LoggerFactory.getLogger(CreateNamespaceRequest.class);

	public CreateNamespaceRequest(String namespace, String repositoryURL, Properties namespaceProperties,
			RDFUploaderContext context) {
		super();
		this.namespace = namespace;
		this.repositoryURL = repositoryURL;
		this.namespaceProperties = namespaceProperties;
		this.context = context;
	}

	public String getTargetNamespace() {
		return namespace;
	}

	public String getRepositoryURL() {
		return repositoryURL;
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	public void setRepositoryURL(String repositoryURL) {
		this.repositoryURL = repositoryURL;
	}

	public Properties getNamespaceProperties() {
		return namespaceProperties;
	}

	public void setNamespaceProperties(Properties namespaceProperties) {
		this.namespaceProperties = namespaceProperties;
	}

	public void accomplishRequest() throws Exception {
		logger.debug("Create Namespace Request");
		RemoteRepositoryManager manager = new RemoteRepositoryManager(this.getRepositoryURL());
		this.context.getBlazegraphClient().createAndGetRemoteRepositoryForNamespace(manager, namespace,
				namespaceProperties);
		manager.close();
		logger.trace("Create Namespace Request.. accomplished");
	}

}
