
package eu.spice.rdfuploader.sandboxes;

import eu.spice.rdfuploader.clients.DBManagementClient;

public class DBManagementClientSandbox {

	private static final String TEST_DATASET = "test_java";
	private static final String TEST_KEY = "test_java";

	private static void createTestDataset(DBManagementClient client) {
		if (!client.getDatasets().contains(TEST_DATASET)) {
			client.createDataset(TEST_DATASET, TEST_KEY);
		}
	}

	public static void main(String[] args) {
		DBManagementClient client = new DBManagementClient("datahub-admin", "DATAHUB1234567890", "http",
				"spice-apif.local");
		createTestDataset(client);
	}

}
