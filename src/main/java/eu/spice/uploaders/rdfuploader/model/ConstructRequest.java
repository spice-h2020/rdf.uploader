
package eu.spice.uploaders.rdfuploader.model;

import java.util.Properties;

import org.apache.jena.rdf.model.Model;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;

import eu.spice.rdfuploader.Constants;
import eu.spice.rdfuploader.Constants.RDFJobsConstants;
import eu.spice.rdfuploader.RDFUploaderContext;
import eu.spice.rdfuploader.uploaders.Utils;

public class ConstructRequest implements Request {

	private JSONObject jobj;
	private String docIdJob;
	private RDFUploaderContext context;

	private static final Logger logger = LoggerFactory.getLogger(ConstructRequest.class);

	public ConstructRequest(JSONObject jobj, String docIdJob, RDFUploaderContext context) {
		super();
		this.jobj = jobj;
		this.docIdJob = docIdJob;
		this.context = context;
	}

	@Override
	public String getTargetNamespace() {
		return jobj.getString(RDFJobsConstants.TARGET_NAMESPACE);
	}

	@Override
	public String getRepositoryURL() {
		return context.getBlazegraphClient().getRepositoryURL();
	}

	@Override
	public void accomplishRequest() throws Exception {

		// Run the query
		String query = jobj.getString(Constants.RDFJobsConstants.QUERY);
		logger.trace("Job type {}, query: {}", jobj.getString("job-type"), query);

		try {

			String namespace = context.getBlazegraphNamespace(jobj.getString(RDFJobsConstants.DATASET));
			Properties namespaceProperties = Utils.loadProperties(context.getConf().getBlazegraphPropertiesFilepath());

			// Executing query
			logger.trace("Executing query");
			Model m = context.getBlazegraphClient().executeConstructQuery(query, namespace);
			logger.trace("Number of triples generated {}", m.size());

			// Minting graphURI
			String graphURI = getGraphURI();
			logger.trace("Target named graph {}", graphURI);

			// Checking if the target namespace exists
			checkNamespaceExists(namespace, namespaceProperties);

			// Clear target graph if requested
			checkClearGraph(graphURI, namespace, namespaceProperties);

			// Uploading
			context.getBlazegraphClient().uploadModel(m, namespace, graphURI, namespaceProperties);

			jobj.put(RDFJobsConstants.STATUS, RDFJobsConstants.COMPLETE);

		} catch (Exception e) {

			logger.error("Error while processing job {}", e.getMessage());
			Utils.addMessage(jobj, e.getMessage());
			jobj.put(RDFJobsConstants.STATUS, RDFJobsConstants.ERROR);

		}
		context.getDbClient().updateDocument(context.getConf().getRDFJobsDataset(), docIdJob, jobj);
	}

	String getGraphURI() {
		String graphURI;
		if (jobj.has(RDFJobsConstants.TARGET_NAMED_GRAPH)) {
			graphURI = jobj.getString(RDFJobsConstants.TARGET_NAMED_GRAPH);
		} else {
			graphURI = context.getGraphURI(jobj.getString(RDFJobsConstants.DATASET), docIdJob);
		}
		return graphURI;
	}

	void checkNamespaceExists(String namespace, Properties namespaceProperties) throws Exception {
		if (!context.getBlazegraphClient().namespaceExists(namespace)) {
			logger.trace("Namespace {} doesn't exist.. creating it", namespace);
			RemoteRepositoryManager manager = new RemoteRepositoryManager(this.getRepositoryURL());
			this.context.getBlazegraphClient().createAndGetRemoteRepositoryForNamespace(manager, namespace,
					namespaceProperties);
			manager.close();
		}
	}

	void checkClearGraph(String graphURI, String namespace, Properties namespaceProperties) throws Exception {
		if (jobj.getBoolean(RDFJobsConstants.CLEAR_GRAPH)) {
			logger.trace("Clearing graph");
			this.context.getBlazegraphClient().clearGraph(namespace, namespaceProperties, graphURI);
		}
	}

}
