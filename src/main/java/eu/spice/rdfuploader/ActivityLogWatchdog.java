package eu.spice.rdfuploader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.function.Predicate;

import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.spice.rdfuploader.Constants.RDFJobsConstants;
import eu.spice.rdfuploader.uploaders.Utils;
import eu.spice.uploaders.rdfuploader.model.ConstructRequest;
import eu.spice.uploaders.rdfuploader.model.CreateNamespaceRequest;
import eu.spice.uploaders.rdfuploader.model.JSONRequestCreate;
import eu.spice.uploaders.rdfuploader.model.JSONRequestDelete;
import eu.spice.uploaders.rdfuploader.model.JSONRequestUpdate;
import eu.spice.uploaders.rdfuploader.model.Request;

public class ActivityLogWatchdog implements Runnable {

	private static final Logger logger = LoggerFactory.getLogger(ActivityLogWatchdog.class);

	private String lastTimestampFile, repositoryURL, baseResource, ontologyURIPRefix, rdf_jobs_dataset;

	private boolean useNamedresources = true;
	private RDFUploaderContext context;

	private static final String AL_PREFIX = "https://mkdf.github.io/context/activity-log#";
	private static final Resource CREATE_DATASET = ModelFactory.createDefaultModel()
			.createResource(AL_PREFIX + "CreateDataset"),
			CREATE = ModelFactory.createDefaultModel().createResource(AL_PREFIX + "Create"),
			UPDATE = ModelFactory.createDefaultModel().createResource(AL_PREFIX + "Update"),
			DELETE = ModelFactory.createDefaultModel().createResource(AL_PREFIX + "Delete");

	//@f:off
	private static String getLastOperationsQuery =
			"PREFIX al:    <"+AL_PREFIX+"> "
			+ "SELECT DISTINCT ?ale ?datasetId ?docId ?timestamp ?operationType ?payload ?endpoint { "
			+ "?ale al:request ?httpRequest . "
			+ "?ale al:datasetId  ?datasetId . "
			+ "?ale a ?operationType . "
			+ "?ale al:timestamp ?timestamp . "
			+ "OPTIONAL{?ale al:documentId ?docId .}"
			+ "OPTIONAL{?httpRequest al:payload ?payload . }"
			+ "OPTIONAL{?httpRequest al:endpoint ?endpoint . }" // TODO needed to overcome Issue #5 https://github.com/spice-h2020/linked-data-hub-env-docker/issues/5
			+ "FILTER(?operationType IN (al:Create, al:Update, al:Delete, al:CreateDataset))"
			+ "FILTER (?timestamp > 3000)" // TODO needed to overcome the bug related to the timestamp Issue #4 https://github.com/spice-h2020/linked-data-hub-env-docker/issues/4
			+ "} "
			+ "ORDER BY ASC(?timestamp)";
	//@f:on

	private BlockingQueue<Request> requests;
	private Properties blazegraphProperties;

	public ActivityLogWatchdog(RDFUploaderContext context, BlockingQueue<Request> requests) throws IOException {
		logger.trace("constructor invoked");
		this.requests = requests;
		this.context = context;
		init(context.getConf());
	}

	private void init(RDFUploaderConfiguration c) throws IOException {
		logger.trace("init invoked");
		blazegraphProperties = Utils.loadProperties(c.getBlazegraphPropertiesFilepath());
		lastTimestampFile = c.getLastTimestampFile();
		repositoryURL = c.getRepositoryURL();
		baseResource = c.getBaseResource();
		ontologyURIPRefix = c.getOntologyURIPRefix();
		useNamedresources = c.isUseNamedresources();
		rdf_jobs_dataset = c.getRDFJobsDataset();
	}

	@Override
	public void run() {
		logger.trace("Method run() invoked");
		Integer lastTimestamp = null;
		try {
			lastTimestamp = getLastTimestamp();
		} catch (IOException e1) {
			logger.error("Exception while getting last timestamp", e1);
		}
		logger.trace("Last timestamp " + lastTimestamp);

		try {
			logger.trace("Getting activity log entities from browse");
			Model m = context.getDbClient().getActivityLogEntitiesFromBrowse(lastTimestamp);
			logger.trace("Triples within the model {}", m.size());
			QueryExecution qexec = QueryExecutionFactory.create(getLastOperationsQuery, m);
			ResultSet rs = qexec.execSelect();
			while (rs.hasNext()) {
				QuerySolution qs = rs.next();
				lastTimestamp = qs.get("timestamp").asLiteral().getInt();
				logger.trace("Processing " + qs.get("operationType").asResource().getLocalName() + " operation "
						+ qs.get("ale").asResource().getLocalName() + " with timestamp " + lastTimestamp);
				String datasetIdentifier = qs.get("datasetId").asLiteral().getString();
				logger.trace("Request for the dataset {}", datasetIdentifier);
				String blazegraphNamespace = context.getBlazegraphNamespace(datasetIdentifier);
				Resource operationType = qs.get("operationType").asResource();
				if (checkRDFJobs(datasetIdentifier)) {
					enqueueRDFJobRequest(qs, operationType);
				} else {
					enqueueRDFUploaderRequest(qs, datasetIdentifier, blazegraphNamespace, operationType);
				}
			}
			saveLastTimestamp(lastTimestamp);
		} catch (Exception e) {
			logger.error("Exception while querying the activity log", e);
			e.printStackTrace();
		}
	}

	void enqueueRDFUploaderRequest(QuerySolution qs, String datasetIdentifier, String blazegraphNamespace,
			Resource operationType) throws InterruptedException {
		if (operationType.equals(CREATE_DATASET)) {
			logger.trace("Create Dataset");
			this.requests
					.put(new CreateNamespaceRequest(blazegraphNamespace, repositoryURL, blazegraphProperties, context));
		} else if (operationType.equals(CREATE)) {
			String payload = qs.get("payload").asLiteral().getString();
			createDocumentRequest(qs, datasetIdentifier, blazegraphNamespace, payload);
		} else if (operationType.equals(DELETE)) {
			logger.trace("Delete Document");
			// TODO remove this as soon as issue 5 is addressed
			String endpoint = qs.get("endpoint").asLiteral().getString();
			String[] split = endpoint.split("/");
			String docId = split[split.length - 1];
			this.requests.put(new JSONRequestDelete(blazegraphNamespace, repositoryURL,
					context.getGraphURI(datasetIdentifier, docId), blazegraphProperties, context));
			logger.trace("{}", this.requests.size());
		} else if (operationType.equals(UPDATE)) {
			logger.trace("Update Document");
			String payload = qs.get("payload").asLiteral().getString();
			String docId = qs.get("docId").asLiteral().getString();
			JSONRequestUpdate request = new JSONRequestUpdate(blazegraphNamespace, repositoryURL,
					context.getGraphURI(datasetIdentifier, docId), blazegraphProperties, new JSONObject(payload),
					getOntologyURIPrefix(datasetIdentifier, docId), context);
			if (useNamedresources) {
				logger.trace("Use named resources");
				request.setRootResourceURI(getRootURI(datasetIdentifier, docId));
			}
			this.requests.put(request);
		}
	}

	void enqueueRDFJobRequest(QuerySolution qs, Resource operationType) {
		String docId = qs.get("docId").asLiteral().getString();
		String payload = qs.get("payload").asLiteral().getString();
		if (operationType.equals(CREATE_DATASET)) {
			// DO NOP
		} else if (operationType.equals(CREATE)) {
			createConstructJobRequest(docId, payload);
		} else if (operationType.equals(DELETE)) {
			removingJob(docId);
		} else if (operationType.equals(UPDATE)) {
			removingJob(docId);
			createConstructJobRequest(docId, payload);
		}
	}

	void removingJob(String docId) {
		logger.trace("Removing job {}", docId);
		boolean removed = requests.removeIf(new Predicate<Request>() {
			@Override
			public boolean test(Request t) {
				return t.getDocId() != null && t.getDocId().equals(docId);
			}
		});
		logger.trace("Job {} removed? {}", docId, removed);
	}

	void createConstructJobRequest(String docId, String payload) {
		logger.trace("Create Job");
		logger.trace("DocId {} Payload {}", docId, payload);
		JSONObject jobj = new JSONObject(payload);

		// If the status of the request is pending, then create a job and put it into
		// the queue.
		if (jobj.getString(RDFJobsConstants.STATUS).equals(RDFJobsConstants.PENDING)) {

			// Update status of the job to processing
			jobj.put(Constants.RDFJobsConstants.STATUS, Constants.RDFJobsConstants.PROCESSING);
			context.getDbClient().updateDocument(rdf_jobs_dataset, docId, jobj);

			try {
				ConstructRequest cr = new ConstructRequest(jobj, docId, this.context);
				this.requests.put(cr);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}

	}

	void createDocumentRequest(QuerySolution qs, String datasetIdentifier, String blazegraphNamespace, String payload)
			throws InterruptedException {
		logger.trace("Create Document");
		String docId = qs.get("docId").asLiteral().getString();
		JSONRequestCreate request = new JSONRequestCreate(blazegraphNamespace, repositoryURL,
				context.getGraphURI(datasetIdentifier, docId), blazegraphProperties, new JSONObject(payload),
				getOntologyURIPrefix(datasetIdentifier, docId), context);
		if (useNamedresources) {
			logger.trace("Use named resources");
			request.setRootResourceURI(getRootURI(datasetIdentifier, docId));
		}
		this.requests.put(request);
	}

	private boolean checkRDFJobs(String datasetIdentifier) {
		return datasetIdentifier.equals(rdf_jobs_dataset);
	}

	private String getRootURI(String datasetId, String docId) {
		return baseResource + datasetId + "/" + docId;
	}

	private String getOntologyURIPrefix(String datasetId, String docId) {
		return ontologyURIPRefix + datasetId + "/" + docId + "/";
	}

	private Integer getLastTimestamp() throws IOException {
		logger.trace("Method getLastTimestamp invoked");
		File timestamp = new File(lastTimestampFile);
		if (timestamp.exists()) {
			BufferedReader br = new BufferedReader(new FileReader(timestamp));
			String line = br.readLine();
			br.close();
			if (line != null) {
				// If the file is corrupted this should throw a NumberFormatException
				try {
					return Integer.parseInt(line);
				} catch (NumberFormatException e11) {
					logger.error("Corrupted timestamp file (ignored)");
				}
			}
		}
		logger.trace("Last timestamp is null");
		return null;
	}

	private void saveLastTimestamp(Integer timestamp) throws IOException {
		logger.trace("Method saveLastTimestamp invoked");
		File timestampFile = new File(lastTimestampFile);
		FileOutputStream fos = new FileOutputStream(timestampFile);
		fos.write((timestamp + "\n").getBytes());
		fos.close();
	}

}
