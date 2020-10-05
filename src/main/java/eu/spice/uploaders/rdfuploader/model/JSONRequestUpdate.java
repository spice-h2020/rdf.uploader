package eu.spice.uploaders.rdfuploader.model;

import java.util.Properties;

import org.json.JSONObject;

public class JSONRequestUpdate extends JSONRequest {

	private JSONObject payload;
	private String ontologyURIPrefix, rootResourceURI;

	public JSONRequestUpdate(String namespace, String repositoryURL, String graphURI, Properties namespaceProperties, JSONObject payload, String ontologyURIPrefix) {
		super(namespace, repositoryURL, graphURI, namespaceProperties);
		this.payload = payload;
		this.ontologyURIPrefix = ontologyURIPrefix;
	}

	public JSONObject getPayload() {
		return payload;
	}

	public void setPayload(JSONObject payload) {
		this.payload = payload;
	}

	public String getOntologyURIPrefix() {
		return ontologyURIPrefix;
	}

	public void setOntologyURIPrefix(String ontologyURIPrefix) {
		this.ontologyURIPrefix = ontologyURIPrefix;
	}

	public String getRootResourceURI() {
		return rootResourceURI;
	}

	public void setRootResourceURI(String resourceURIPrefix) {
		this.rootResourceURI = resourceURIPrefix;
	}

}
