package eu.spice.rdfuploader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
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
import eu.spice.uploaders.rdfuploader.model.ConstructRequest;
import eu.spice.uploaders.rdfuploader.model.CreateFileRequest;
import eu.spice.uploaders.rdfuploader.model.CreateNamespaceRequest;
import eu.spice.uploaders.rdfuploader.model.JSONRequestCreate;
import eu.spice.uploaders.rdfuploader.model.JSONRequestDelete;
import eu.spice.uploaders.rdfuploader.model.JSONRequestUpdate;
import eu.spice.uploaders.rdfuploader.model.RebuildGraphRequest;
import eu.spice.uploaders.rdfuploader.model.RebuildNamespaceRequest;
import eu.spice.uploaders.rdfuploader.model.Request;

public class ActivityLogWatchdog implements Runnable {

	private static final Logger logger = LoggerFactory.getLogger(ActivityLogWatchdog.class);

	private String lastTimestampFile;

	private RDFUploaderContext context;

	private static final String AL_PREFIX = "https://mkdf.github.io/context/activity-log#";
	private static final Resource CREATE_DATASET = ModelFactory.createDefaultModel()
			.createResource(AL_PREFIX + "CreateDataset"),
			CREATE = ModelFactory.createDefaultModel().createResource(AL_PREFIX + "Create"),
			UPDATE = ModelFactory.createDefaultModel().createResource(AL_PREFIX + "Update"),
			DELETE = ModelFactory.createDefaultModel().createResource(AL_PREFIX + "Delete"),
			CREATE_FILE = ModelFactory.createDefaultModel().createResource(AL_PREFIX + "CreateFile");

	// @f:off
	private static String getLastOperationsQuery =
			"PREFIX al:    <"+AL_PREFIX+"> "
			+ "SELECT DISTINCT ?ale ?datasetId ?docId ?timestamp ?operationType ?payload ?endpoint ?filename { "
			+ "?ale al:request ?httpRequest . "
			+ "?ale al:datasetId  ?datasetId . "
			+ "?ale a ?operationType . "
			+ "?ale al:timestamp ?timestamp . "
			+ "OPTIONAL {?ale al:filename ?filename . }"
			+ "?httpRequest al:agent ?agent . FILTER NOT EXISTS {?agent al:key \"datahub-admin\"} "
			+ "OPTIONAL{?ale al:documentId ?docId .} "
			+ "OPTIONAL{?httpRequest al:payload ?payload . } "
			+ "OPTIONAL{?httpRequest al:endpoint ?endpoint . } " // TODO needed to overcome Issue #5 https://github.com/spice-h2020/linked-data-hub-env-docker/issues/5
			+ "FILTER(?operationType IN (al:Create, al:Update, al:Delete, al:CreateDataset, al:CreateFile)) "
			+ "FILTER (?timestamp > 3000) " // TODO needed to overcome the bug related to the timestamp Issue #4 https://github.com/spice-h2020/linked-data-hub-env-docker/issues/4
			+ "} "
			+ "ORDER BY ASC(?timestamp)";
	// @f:on

	private BlockingQueue<Request> requests;

	public ActivityLogWatchdog(RDFUploaderContext context, BlockingQueue<Request> requests) throws IOException {
		logger.trace("constructor invoked");
		this.requests = requests;
		this.context = context;
		lastTimestampFile = context.getConf().getLastTimestampFile();
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
		logger.trace("Last timestamp {} number of requests {}", lastTimestamp, this.requests.size());
		try {
			logger.trace("Getting activity log entities from browse");
			Model m = context.getDbClient().getActivityLogEntitiesFromBrowse(lastTimestamp);
			logger.trace("Triples within the model {}", m.size());
			QueryExecution qexec = QueryExecutionFactory.create(getLastOperationsQuery, m);
			ResultSet rs = qexec.execSelect();
			qexec = QueryExecutionFactory.create(getLastOperationsQuery, m);
			while (rs.hasNext()) {
				QuerySolution qs = rs.next();
				lastTimestamp = qs.get("timestamp").asLiteral().getInt();
				logger.trace("Processing " + qs.get("operationType").asResource().getLocalName() + " operation "
						+ qs.get("ale").asResource().getLocalName() + " with timestamp " + lastTimestamp);
				String datasetIdentifier = qs.get("datasetId").asLiteral().getString();
				Resource operationType = qs.get("operationType").asResource();
				logger.trace("Request for the dataset {} - {}", datasetIdentifier, operationType);
				if (datasetIdentifier.equals(context.getConf().getRDFJobsDataset())) {
					if (!operationType.equals(CREATE_DATASET)) {
						enqueueRDFJobRequest(qs, operationType);
					}
				} else {
					enqueueRDFUploaderRequest(qs, datasetIdentifier, operationType);
				}
			}
			saveLastTimestamp(lastTimestamp);
		} catch (Exception e) {
			logger.error("Exception while querying the activity log", e);
			e.printStackTrace();
		}
	}

	void enqueueRDFUploaderRequest(QuerySolution qs, String datasetIdentifier, Resource operationType)
			throws InterruptedException {
		logger.trace("Processing {} activity", operationType.getLocalName());
		if (operationType.equals(CREATE_DATASET)) {
			this.requests.put(new CreateNamespaceRequest(datasetIdentifier, context));
		} else if (operationType.equals(CREATE)) {
			String payload = qs.get("payload").asLiteral().getString();
			logger.trace("Payload {}", payload);
			String docId = qs.get("docId").asLiteral().getString();
			try {
				JSONObject payloadObject = new JSONObject(payload);
				JSONRequestCreate request = new JSONRequestCreate(datasetIdentifier, docId, payloadObject, context);
				this.requests.put(request);
			} catch (org.json.JSONException e) {
				e.printStackTrace();
				logger.error("Error {} while processing the payload {}", e.getMessage(), payload);
			}
		} else if (operationType.equals(DELETE)) {
			// TODO remove this as soon as issue 5 is addressed
			String endpoint = qs.get("endpoint").asLiteral().getString();
			String[] split = endpoint.split("/");
			String docId = split[split.length - 1];
			this.requests.put(new JSONRequestDelete(datasetIdentifier, docId, context));
		} else if (operationType.equals(UPDATE)) {
			logger.trace("Update Document");
			String payload = qs.get("payload").asLiteral().getString();
			String docId = qs.get("docId").asLiteral().getString();
			JSONRequestUpdate request = new JSONRequestUpdate(datasetIdentifier, docId, new JSONObject(payload),
					context);
			this.requests.put(request);
		} else if (operationType.equals(CREATE_FILE)) {
			String filename = qs.get("filename").asLiteral().getString().toString();
			logger.trace("Filename  {}", filename);
			CreateFileRequest r = new CreateFileRequest(filename, datasetIdentifier, context);
			this.requests.put(r);
		}
	}

	void enqueueRDFJobRequest(QuerySolution qs, Resource operationType) {
		logger.trace("Payload {}", qs.get("payload").asLiteral().getString());
		JSONObject payload = new JSONObject(qs.get("payload").asLiteral().getString());
		String docId = qs.get("docId").asLiteral().getString();

		if (payload.getString(RDFJobsConstants.JOB_TYPE).equals(RDFJobsConstants.CONSTRUCT)) {
			logger.trace("Construct JOB");
			if (operationType.equals(CREATE)) {
				createConstructJobRequest(docId, payload);
			} else if (operationType.equals(DELETE)) {
				removingJob(docId);
			} else if (operationType.equals(UPDATE)) {
				removingJob(docId);
				createConstructJobRequest(docId, payload);
			}
		} else if (payload.getString(RDFJobsConstants.JOB_TYPE).equals(RDFJobsConstants.REBUILDGRAPH)) {
			logger.trace("REBUILD GRAPH JOB");
			if (operationType.equals(CREATE)) {
				createRebuildGraphJobRequest(docId, payload);
			} else if (operationType.equals(DELETE)) {
				removingJob(docId);
			} else if (operationType.equals(UPDATE)) {
				removingJob(docId);
				createRebuildGraphJobRequest(docId, payload);
			}
		} else if (payload.getString(RDFJobsConstants.JOB_TYPE).equals(RDFJobsConstants.REBUILD_DATASET)) {
			logger.trace("REBUILDNAMESPACE GRAPH JOB");
			if (operationType.equals(CREATE)) {
				createRebuildNamespaceJobRequest(docId, payload);
			} else if (operationType.equals(DELETE)) {
				removingJob(docId);
			} else if (operationType.equals(UPDATE)) {
				removingJob(docId);
				createRebuildNamespaceJobRequest(docId, payload);
			}
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

	void createConstructJobRequest(String docId, JSONObject jobj) {
		logger.trace("Create Job");
		logger.trace("DocId {} Payload {}", docId, jobj);
		if (jobj.getString(RDFJobsConstants.STATUS).equals(RDFJobsConstants.PENDING)) {

			jobj.put(Constants.RDFJobsConstants.STATUS, Constants.RDFJobsConstants.PROCESSING);
			context.getDbClient().updateDocument(context.getConf().getRDFJobsDataset(), docId, jobj);

			try {
				ConstructRequest cr = new ConstructRequest(docId, jobj, this.context);
				this.requests.put(cr);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	void createRebuildGraphJobRequest(String docId, JSONObject jobj) {
		logger.trace("Creting rebuild graph job");
		logger.trace("DocId {} Payload {}", docId, jobj);
		if (jobj.getString(RDFJobsConstants.STATUS).equals(RDFJobsConstants.PENDING)) {
			jobj.put(Constants.RDFJobsConstants.STATUS, Constants.RDFJobsConstants.PROCESSING);
			context.getDbClient().updateDocument(context.getConf().getRDFJobsDataset(), docId, jobj);
			try {
				RebuildGraphRequest r = new RebuildGraphRequest(docId, jobj, this.context);
				this.requests.put(r);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	void createRebuildNamespaceJobRequest(String docId, JSONObject jobj) {
		logger.trace("Creting rebuild namespace job");
		logger.trace("DocId {} Payload {}", docId, jobj);
		if (jobj.getString(RDFJobsConstants.STATUS).equals(RDFJobsConstants.PENDING)) {
			jobj.put(Constants.RDFJobsConstants.STATUS, Constants.RDFJobsConstants.PROCESSING);
			context.getDbClient().updateDocument(context.getConf().getRDFJobsDataset(), docId, jobj);
			try {
				RebuildNamespaceRequest r = new RebuildNamespaceRequest(docId, jobj, this.context);
				this.requests.put(r);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
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
