package eu.spice.uploaders.rdfuploader.model;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;

import eu.spice.rdfuploader.uploaders.Utils;

public class JSONRequestDelete extends JSONRequest {

	private static final Logger logger = LoggerFactory.getLogger(JSONRequestDelete.class);

	public JSONRequestDelete(String namespace, String repositoryURL, String graphURI, Properties namespaceProperties) {
		super(namespace, repositoryURL, graphURI, namespaceProperties);
	}

	public void accomplishRequest() throws Exception {
		logger.debug("CLEAR graph " + this.getGraphURI());
		RemoteRepositoryManager manager = new RemoteRepositoryManager(this.getRepositoryURL());
		RemoteRepository rr = Utils.createAndGetRemoteRepositoryForNamespace(manager, this.getTargetNamespace(),
				this.getNamespaceProperties());
		rr.prepareUpdate("CLEAR GRAPH <" + this.getGraphURI() + ">").evaluate();
		logger.trace("CLEAR graph " + this.getGraphURI() + " completed!");
		manager.close();
	}

}
