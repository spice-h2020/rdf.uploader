package eu.spice.uploaders.rdfuploader.model;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.spice.rdfuploader.RDFUploaderContext;

public class JSONRequestDelete extends JSONRequest {

	private static final Logger logger = LoggerFactory.getLogger(JSONRequestDelete.class);

	public JSONRequestDelete(String namespace, String repositoryURL, String graphURI, Properties namespaceProperties,
			RDFUploaderContext context) {
		super(namespace, repositoryURL, graphURI, namespaceProperties, context);
	}

	public void accomplishRequest() throws Exception {
		logger.debug("CLEAR graph " + this.getGraphURI());
		super.getContext().getBlazegraphClient().clearGraph(getTargetNamespace(), getNamespaceProperties(),
				getGraphURI());
	}

	@Override
	public String getDocId() {
		// TODO Auto-generated method stub
		return null;
	}

}
