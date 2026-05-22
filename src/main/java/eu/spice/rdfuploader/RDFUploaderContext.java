package eu.spice.rdfuploader;

import java.io.IOException;

import eu.spice.rdfuploader.clients.BlazegraphClient;
import eu.spice.rdfuploader.clients.DocumentDBClient;
import eu.spice.rdfuploader.clients.SPARQLAnythingClient;
import eu.spice.rdfuploader.clients.TripleStoreClient;

public class RDFUploaderContext {

	private RDFUploaderConfiguration conf;
	private DocumentDBClient dbClient;
	private TripleStoreClient tripleStoreClient;
	private SPARQLAnythingClient saClient;

	public RDFUploaderContext(RDFUploaderConfiguration conf) throws IOException {
		this.dbClient = new DocumentDBClient(conf.getUsername(), conf.getPassword(), conf.getApif_uri_scheme(),
				conf.getApif_host(), conf.getActivity_log_path(), conf.getBaseNS(), conf.getPagesize());
		this.tripleStoreClient = createTripleStoreClient(conf);
		this.conf = conf;
		this.saClient = SPARQLAnythingClient.getInstance(conf);
	}

	private TripleStoreClient createTripleStoreClient(RDFUploaderConfiguration conf) {
		String backend = conf.getBackend();
		switch (backend) {
			case "blazegraph":
				return new BlazegraphClient(conf.getRepositoryURL(), conf.getBlazegraphNamespacePrefix(),
						conf.getBlazegraphPropertiesFilepath());
			case "virtuoso":
				throw new UnsupportedOperationException("Virtuoso backend not yet implemented");
			default:
				throw new IllegalArgumentException("Unknown backend: " + backend);
		}
	}

	public RDFUploaderConfiguration getConf() {
		return conf;
	}

	public DocumentDBClient getDbClient() {
		return dbClient;
	}

	public TripleStoreClient getTripleStoreClient() {
		return tripleStoreClient;
	}

	public String getNamespace(String datasetIdentifier) {
		return conf.getBlazegraphNamespacePrefix() + datasetIdentifier;
	}

	public String getGraphURI(String datasetId, String docId) {
		return conf.getBaseGraph() + datasetId + "/" + docId;
	}

	public String getDocIdFromGraphURI(String graphURI) {
		String[] split = graphURI.split("/");
		return split[split.length - 1];
	}

	public String getDatasetIdFromGraphURI(String graphURI) {
		String[] split = graphURI.split("/");
		return split[split.length - 2];
	}

	public String getRootURI(String datasetId, String docId) {
		return conf.getBaseResource() + datasetId + "/" + docId;
	}

	public String getOntologyURIPrefix(String datasetId, String docId) {
		return conf.getOntologyURIPRefix() + datasetId + "/" + docId + "/";
	}

	public SPARQLAnythingClient getSPARQLAnythingClient() {
		return saClient;
	}

	public boolean isSkipRDFJobs() {
		return conf.isSkipRDFJobs();
	}

	public boolean isDisableWriting() {
		return conf.isDisableWriting();
	}
}