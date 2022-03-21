
package eu.spice.uploaders.rdfuploader.model;

import java.util.Properties;

import org.apache.jena.rdf.model.Model;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.spice.rdfuploader.Constants;
import eu.spice.rdfuploader.Constants.RDFJobsConstants;
import eu.spice.rdfuploader.RDFUploaderContext;
import eu.spice.rdfuploader.uploaders.Utils;

public class RebuildGraphRequest implements Request {

	private RDFUploaderContext context;
	private JSONObject job;
	private String jobId;
	private static final Logger logger = LoggerFactory.getLogger(RebuildGraphRequest.class);

	public RebuildGraphRequest(String jobId, JSONObject job, RDFUploaderContext context) {
		super();
		this.context = context;
		this.job = job;
		this.jobId = jobId;
	}

	public String getTargetNamespace() {
		return context.getBlazegraphNamespace(job.getString(RDFJobsConstants.DATASET));
	}

	@Override
	public String getDocId() {
		return jobId;
	}

	public String getRepositoryURL() {
		return context.getBlazegraphClient().getRepositoryURL();
	}

	@Override
	public void accomplishRequest() {

		String documentId = job.getString(RDFJobsConstants.DOCUMENT_ID);
		String datasetId = job.getString(RDFJobsConstants.DATASET);
		JSONObject obj = context.getDbClient().retrieveDocument(datasetId, documentId);

		logger.trace("Rebuilding graph for docId {} in dataset {}", documentId, datasetId);

		if (obj != null) {
			logger.trace("Triplifying document");
			Model m;
			try {
//				m = Utils.readOrTriplifyJSONObject(obj, context.getRootURI(datasetId, documentId),
//						context.getOntologyURIPrefix(datasetId, documentId));
				m = Utils.readOrTriplifyJSONObject(obj, context.getRootURI(datasetId, documentId));
				logger.trace("Model size: {}", m.size());
				logger.trace("Clearing and uploading model");
				String graphURI = context.getGraphURI(datasetId, documentId);
				Properties blazegraphProperties = Utils
						.loadProperties(context.getConf().getBlazegraphPropertiesFilepath());
				context.getBlazegraphClient().uploadModel(m, getTargetNamespace(), graphURI, blazegraphProperties,
						true);
				job.put(Constants.RDFJobsConstants.STATUS, Constants.RDFJobsConstants.COMPLETE);
				logger.trace("Request accomplished");
			} catch (Exception e) {
				logger.error("Error while processing job {}", e.getMessage());
				Utils.addMessage(job, e.getMessage());
				job.put(RDFJobsConstants.STATUS, RDFJobsConstants.ERROR);
			}

		} else {
			logger.error("Couldn't find document {}" + documentId + " " + datasetId);
			Utils.addMessage(job, "Couldn't find document {}" + documentId + " " + datasetId);
			job.put(RDFJobsConstants.STATUS, RDFJobsConstants.ERROR);
		}
		context.getDbClient().updateDocument(context.getConf().getRDFJobsDataset(), jobId, job);

	}

}
