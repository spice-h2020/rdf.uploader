
package eu.spice.rdfuploader;

public class RDFUploaderContext {

	private RDFUploaderConfiguration conf;
	private DocumentDBClient dbClient;
	private BlazegraphClient blazegraphClient;

	public RDFUploaderContext(RDFUploaderConfiguration conf) {
		this.dbClient = new DocumentDBClient(conf.getUsername(), conf.getPassword(), conf.getApif_uri_scheme(),
				conf.getApif_host(), conf.getActivity_log_path(), conf.getBaseNS());
		this.blazegraphClient = new BlazegraphClient(conf.getRepositoryURL(), conf.getBlazegraphNamespacePrefix());
		this.conf = conf;
	}

	public RDFUploaderConfiguration getConf() {
		return conf;
	}

	public DocumentDBClient getDbClient() {
		return dbClient;
	}

	public BlazegraphClient getBlazegraphClient() {
		return blazegraphClient;
	}

	public String getBlazegraphNamespace(String datasetIdentifier) {
		return conf.getBlazegraphNamespacePrefix() + datasetIdentifier;
	}

	public String getGraphURI(String datasetId, String docId) {
		return conf.getBaseGraph() + datasetId + "/" + docId;
	}
}
