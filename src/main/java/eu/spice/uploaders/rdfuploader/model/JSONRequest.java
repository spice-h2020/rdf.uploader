package eu.spice.uploaders.rdfuploader.model;

import java.util.Properties;

public class JSONRequest implements Request {

	private String namespace;
	private String repositoryURL;
	private String graphURI;
	private Properties namespaceProperties;

	public JSONRequest(String namespace, String repositoryURL, String graphURI, Properties namespaceProperties) {
		super();
		this.namespace = namespace;
		this.repositoryURL = repositoryURL;
		this.graphURI = graphURI;
		this.namespaceProperties = namespaceProperties;
	}

	public String getNamespace() {
		return namespace;
	}

	public String getRepositoryURL() {
		return repositoryURL;
	}

	public String getGraphURI() {
		return graphURI;
	}

	public Properties getNamespaceProperties() {
		return namespaceProperties;
	}

}
