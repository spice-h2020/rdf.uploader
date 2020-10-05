package eu.spice.uploaders.rdfuploader.model;

import java.util.Properties;

public class CreateNamespaceRequest implements Request {

	private String namespace, repositoryURL;
	private Properties namespaceProperties;

	public CreateNamespaceRequest(String namespace, String repositoryURL, Properties namespaceProperties) {
		super();
		this.namespace = namespace;
		this.repositoryURL = repositoryURL;
		this.namespaceProperties = namespaceProperties;
	}

	@Override
	public String getNamespace() {
		return namespace;
	}

	@Override
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

}
