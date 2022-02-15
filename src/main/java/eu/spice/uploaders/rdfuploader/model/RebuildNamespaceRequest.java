
package eu.spice.uploaders.rdfuploader.model;

import java.util.Properties;

import org.apache.jena.rdf.model.Model;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;

import eu.spice.rdfuploader.Constants.RDFJobsConstants;
import eu.spice.rdfuploader.RDFUploaderContext;
import eu.spice.rdfuploader.uploaders.Utils;

public class RebuildNamespaceRequest implements Request {

	private RDFUploaderContext context;
	private JSONObject job;
	private String jobId;
	private static final Logger logger = LoggerFactory.getLogger(RebuildNamespaceRequest.class);

	public RebuildNamespaceRequest(String jobId, JSONObject job, RDFUploaderContext context) {
		super();
		this.context = context;
		this.job = job;
		this.jobId = jobId;
	}

	@Override
	public String getTargetNamespace() {
		return context.getBlazegraphNamespace(job.getString(RDFJobsConstants.DATASET));
	}

	@Override
	public String getDocId() {
		return jobId;
	}

	@Override
	public String getRepositoryURL() {
		return context.getBlazegraphClient().getRepositoryURL();
	}

	@Override
	public void accomplishRequest() {

		String namespace = context.getBlazegraphNamespace(job.getString(RDFJobsConstants.DATASET));

		logger.trace("Dropping namespace {}", namespace);
		try {
			// Drop existing namespace
			context.getBlazegraphClient().dropNamespace(namespace);
			Utils.addMessage(job, "Namespace deleted");

			// Recreate namespace
			RemoteRepositoryManager repo = new RemoteRepositoryManager(this.getRepositoryURL());
			context.getBlazegraphClient().createAndGetRemoteRepositoryForNamespace(repo, namespace,
					Utils.loadProperties(context.getConf().getBlazegraphPropertiesFilepath()));
			Utils.addMessage(job, "Namespace created");

			// Retrieve documents
			JSONArray documents = context.getDbClient().retrieveDocuments(job.getString(RDFJobsConstants.DATASET));
			final Properties blazegraphProperties = Utils
					.loadProperties(context.getConf().getBlazegraphPropertiesFilepath());

			// Triplify and upload documents
			documents.forEach(obj -> {

				JSONObject jsonObj = ((JSONObject) obj);
				String documentId = jsonObj.getString("_id");
				String datasetId = job.getString(RDFJobsConstants.DATASET);
				String ontologyURI = context.getOntologyURIPrefix(datasetId, documentId);
				String rootURI = context.getRootURI(job.getString(RDFJobsConstants.DATASET), documentId);
				String graphURI = context.getGraphURI(datasetId, documentId);

				try {
					Model m = Utils.readOrTriplifyJSONObject((JSONObject) obj, rootURI, ontologyURI);
					context.getBlazegraphClient().uploadModel(m, namespace, graphURI, blazegraphProperties);
					Utils.addMessage(job, documentId + " uploaded");
				} catch (Exception e) {
					logger.error("Error while processing job {}", e.getMessage());
					Utils.addMessage(job, e.getMessage());
				}

			});

			job.put(RDFJobsConstants.STATUS, RDFJobsConstants.COMPLETE);

		} catch (Exception e) {
			logger.error("Error while processing job {}", e.getMessage());
			Utils.addMessage(job, e.getMessage());
			job.put(RDFJobsConstants.STATUS, RDFJobsConstants.ERROR);
		}

		context.getDbClient().updateDocument(context.getConf().getRDFJobsDataset(), jobId, job);

	}

}
