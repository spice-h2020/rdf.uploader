
package eu.spice.rdfuploader.sandboxes;

import org.json.JSONObject;

import eu.spice.rdfuploader.RDFUploaderConfiguration;
import eu.spice.rdfuploader.clients.DBManagementClient;
import eu.spice.rdfuploader.clients.DocumentDBClient;

public class DocumentDBClientSandbox {

	private static final String TEST_DATASET = "document_db_client_sandbox_dataset";
	private static final String TEST_KEY = "document_db_client_sandbox_key";

	private static void createTestDataset(DBManagementClient client) {
		if (!client.getDatasets().contains(TEST_DATASET)) {
			client.createDataset(TEST_DATASET, TEST_KEY);
		}
	}

	public static void main(String[] args) {
		RDFUploaderConfiguration c = RDFUploaderConfiguration.getInstance(args[0]);
		DBManagementClient client = new DBManagementClient(c.getUsername(), c.getPassword(), c.getApif_uri_scheme(),
				c.getApif_host());
		createTestDataset(client);
		DocumentDBClient documentDBClient = new DocumentDBClient(TEST_KEY, TEST_KEY, c.getApif_uri_scheme(),
				c.getApif_host(), c.getActivity_log_path(), c.getBaseNS(), c.getPagesize());
		documentDBClient.createDocument(TEST_DATASET, "test_doc",
				new JSONObject("{'_id':'test_doc', 'attr':'attrVal'}"));
		documentDBClient.deleteDocument(TEST_DATASET, "test_doc");
	}

}
