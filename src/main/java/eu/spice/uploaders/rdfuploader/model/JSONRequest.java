package eu.spice.uploaders.rdfuploader.model;

import java.util.Properties;

import eu.spice.rdfuploader.RDFUploaderContext;

public abstract class JSONRequest implements Request {

	private String namespace;
	private String repositoryURL;
	private String graphURI;
	private Properties namespaceProperties;
	private RDFUploaderContext context;

	public JSONRequest(String namespace, String repositoryURL, String graphURI, Properties namespaceProperties,
			RDFUploaderContext context) {
		super();
		this.namespace = namespace;
		this.repositoryURL = repositoryURL;
		this.graphURI = graphURI;
		this.namespaceProperties = namespaceProperties;
		this.context = context;
	}

	public String getTargetNamespace() {
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

	public RDFUploaderContext getContext() {
		return context;
	}

}
