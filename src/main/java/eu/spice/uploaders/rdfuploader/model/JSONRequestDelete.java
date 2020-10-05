package eu.spice.uploaders.rdfuploader.model;

import java.util.Properties;

public class JSONRequestDelete extends JSONRequest {

	public JSONRequestDelete(String namespace, String repositoryURL, String graphURI, Properties namespaceProperties) {
		super(namespace, repositoryURL, graphURI, namespaceProperties);
	}

}
