
package eu.spice.rdfuploader;

import java.io.IOException;

import eu.spice.rdfuploader.clients.BlazegraphClient;
import eu.spice.rdfuploader.clients.DocumentDBClient;
import eu.spice.rdfuploader.clients.SPARQLAnythingClient;

public class RDFUploaderContext {

	private RDFUploaderConfiguration conf;
	private DocumentDBClient dbClient;
	private BlazegraphClient blazegraphClient;
	private SPARQLAnythingClient saClient;

	public RDFUploaderContext(RDFUploaderConfiguration conf) throws IOException {
		this.dbClient = new DocumentDBClient(conf.getUsername(), conf.getPassword(), conf.getApif_uri_scheme(),
				conf.getApif_host(), conf.getActivity_log_path(), conf.getBaseNS());
		this.blazegraphClient = new BlazegraphClient(conf.getRepositoryURL(), conf.getBlazegraphNamespacePrefix());
		this.conf = conf;
		this.saClient = SPARQLAnythingClient.getInstance(conf);
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

}
