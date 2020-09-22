package eu.spice.uploaders.model;

import java.util.Properties;

import org.json.JSONObject;

public class JSONRequest implements Request {

	private String namespace;
	private String repositoryURL;
	private String graphURI;
	private String ontologyURIPrefix, resourceURIPrefix;
	private Properties namespaceProperties;
	private JSONObject payload;

	public JSONRequest(String namespace, String repositoryURL, String graphURI, Properties namespaceProperties,
			JSONObject payload, String ontologyURIPRefix) {
		super();
		this.namespace = namespace;
		this.repositoryURL = repositoryURL;
		this.graphURI = graphURI;
		this.namespaceProperties = namespaceProperties;
		this.payload = payload;
		this.ontologyURIPrefix = ontologyURIPRefix;
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

	public JSONObject getPayload() {
		return payload;
	}

	public String getOntologyURIPrefix() {
		return ontologyURIPrefix;
	}

	public String getResourceURIPrefix() {
		return resourceURIPrefix;
	}

	public void setResourceURIPrefix(String resourceURIPrefix) {
		this.resourceURIPrefix = resourceURIPrefix;
	}

}
