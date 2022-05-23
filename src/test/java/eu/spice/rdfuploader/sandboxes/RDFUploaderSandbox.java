package eu.spice.rdfuploader.sandboxes;

import java.util.ArrayList;
import java.util.List;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.Syntax;
import org.json.JSONObject;

import eu.spice.rdfuploader.BlockingQueueListener;
import eu.spice.rdfuploader.RDFUploader;
import eu.spice.rdfuploader.RDFUploaderConfiguration;
import eu.spice.rdfuploader.clients.DBManagementClient;
import eu.spice.rdfuploader.clients.DocumentDBClient;
import eu.spice.uploaders.rdfuploader.model.Request;

public class RDFUploaderSandbox {

	private static final String TEST_KEY = "rdf_uploader_sandbox_key";

	private static void createTestDataset(DBManagementClient client, String datasetId, String key) {
		if (!client.getDatasets().contains(datasetId)) {
			client.createDataset(datasetId, key);
		}
	}

	private static void waitForRequestToBeAccomplished(BlockingQueueListener<Request> requests, String datasetId)
			throws InterruptedException {
		while (true) {
			Request req = requests.getSideQueue().take();
			if (!req.getDataset().equals(datasetId)) {
				continue;
			}
			// Wait until the request is marked as accomplished
			while (!req.isAccomplished()) {
				System.err.println(req.getClass().toString() + " not accomplished yet");
				Thread.sleep(1000);
			}
			break;

		}
		System.err.println("Request Accomplished");
	}

	private static boolean ask(String queryString, String datasetId, RDFUploaderConfiguration c) {
		Query q = QueryFactory.create(queryString);
		System.err.println(q.toString(Syntax.defaultQuerySyntax));
		String sparqlEndpointURL = c.getRepositoryURL() + "/namespace/" + c.getBlazegraphNamespacePrefix() + datasetId
				+ "/sparql";
		System.err.println("Repository URL: " + sparqlEndpointURL);
		QueryExecution qexec = QueryExecutionFactory.sparqlService(sparqlEndpointURL, q);
		return qexec.execAsk();
	}

	private static String createAsk(String datasetId, String docId, String cond, RDFUploaderConfiguration c) {
		String safeId = Request.basicEscaper.escape(docId);
		String graphURI = String.format("<%s%s/%s>", c.getBaseGraph(), datasetId, safeId);
		String resourceURI = String.format("<%s%s/%s>", c.getBaseResource(), datasetId, safeId);
		return String
				.format("PREFIX fx: <http://sparql.xyz/facade-x/ns/> PREFIX xyz: <http://sparql.xyz/facade-x/data/> "
						+ "ASK { GRAPH %s { %s a fx:root ; %s  } }", graphURI, resourceURI, cond);
	}

	private static List<String> createAndDeleteJSONDocument(DocumentDBClient client,
			BlockingQueueListener<Request> requests, String datasetId, String key, JSONObject doc, String cond,
			RDFUploaderConfiguration c) throws InterruptedException {

		List<String> report = new ArrayList<String>();

		String docId = doc.get("_id").toString();
		client.createDocument(datasetId, docId, doc);

		waitForRequestToBeAccomplished(requests, datasetId);

		String m = String.format("Creation of JSON Document %s : %s", doc.toString(),
				Boolean.toString(ask(createAsk(datasetId, docId, cond, c), datasetId, c)));
		report.add(m);

		client.deleteDocument(datasetId, docId);

		waitForRequestToBeAccomplished(requests, datasetId);

		m = String.format("Deletion of JSON Document %s : %s", doc.toString(),
				Boolean.toString(!ask(createAsk(datasetId, docId, cond, c), datasetId, c)));
		report.add(m);

		return report;

	}

	public static void main(String[] args) throws Exception {

		RDFUploader.testingMode = true;

		Runnable r = () -> {
			try {
				RDFUploader.main(args);
			} catch (Exception e) {
				e.printStackTrace();
			}
		};
		Thread t = new Thread(r);
		t.start();

		BlockingQueueListener<Request> requests = (BlockingQueueListener<Request>) RDFUploader.requests;

		while (requests == null) {
			requests = (BlockingQueueListener<Request>) RDFUploader.requests;
			Thread.sleep(1000);
		}

		while (requests.getSideQueue() == null) {
			Thread.sleep(1000);
		}

		RDFUploaderConfiguration c = RDFUploaderConfiguration.getInstance();

		String dataset_id = "rdf_uploader_sandbox_dataset_" + System.currentTimeMillis();

		DBManagementClient client = new DBManagementClient(c.getUsername(), c.getPassword(), c.getApif_uri_scheme(),
				c.getApif_host());
		createTestDataset(client, dataset_id, TEST_KEY);

		DocumentDBClient documentDBClient = new DocumentDBClient(TEST_KEY, TEST_KEY, c.getApif_uri_scheme(),
				c.getApif_host(), c.getActivity_log_path(), c.getBaseNS(), c.getPagesize());

		JSONObject doc = new JSONObject("{'_id':'id1','attr':'attrValue'}");
		List<String> report = createAndDeleteJSONDocument(documentDBClient, requests, dataset_id, TEST_KEY, doc,
				" xyz:attr \"attrValue\" . ", c);

		doc = new JSONObject("{'_id':1,'attr':'attrValue'}");
		report.addAll(createAndDeleteJSONDocument(documentDBClient, requests, dataset_id, TEST_KEY, doc,
				" xyz:attr \"attrValue\" . ", c));

		doc = new JSONObject("{'_id':'test space','attr':'attrValue'}");
		report.addAll(createAndDeleteJSONDocument(documentDBClient, requests, dataset_id, TEST_KEY, doc,
				" xyz:attr \"attrValue\" . ", c));

		doc = new JSONObject("{'_id':'test space','attr with spaces':'attrValue'}");
		report.addAll(createAndDeleteJSONDocument(documentDBClient, requests, dataset_id, TEST_KEY, doc,
				" xyz:attr%20with%20spaces \"attrValue\" . ", c));

		doc = new JSONObject("{'_id':'test space','dc:description':'attrValue'}");
		report.addAll(createAndDeleteJSONDocument(documentDBClient, requests, dataset_id, TEST_KEY, doc,
				" ; xyz:dc%3Adescription \"attrValue\" . [ <dc:description> \"attrValue\" ]  ", c));

		System.err.println("\n\n\nREPORT\n\n\n");
		for (String s : report) {
			System.err.println(s);
		}

		System.exit(0);

	}

}
