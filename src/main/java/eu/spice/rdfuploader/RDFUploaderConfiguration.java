package eu.spice.rdfuploader;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class RDFUploaderConfiguration {

	private static final Logger logger = LogManager.getLogger(RDFUploaderConfiguration.class);

	private String username = "datahub-admin", password = "DATAHUB1234567890", apif_host = "spice-apif.local",
			lastTimestampFile = "timestamp", apif_uri_scheme = "http", activity_log_path = "/object/activity_log",
			baseNS = "http://spice-apif.local/object/activity_log/", repositoryURL = "http://localhost:9999/blazegraph",
			blazegraphPropertiesFilepath = "src/main/resources/blazegraph.properties",
			baseResource = "https://w3id.org/spice/resource/", baseGraph = baseResource + "graph/",
			ontologyURIPRefix = "https://w3id.org/spice/ontology/", blazegraphNamespacePrefix = "", tmpFolder;

	private boolean useNamedresources = true;

	private int requestQueueSize = 100, lookupRateSeconds = 10;

	private static RDFUploaderConfiguration instance;

	private RDFUploaderConfiguration() {
		try {
			Configurations configs = new Configurations();
			Configuration config = configs.properties("config.properties");
			logger.trace(ConfigurationUtils.toString(config));

			username = config.getString("username");
			password = config.getString("password");
			apif_host = config.getString("apif_host");
			lastTimestampFile = config.getString("lastTimestampFile");
			apif_uri_scheme = config.getString("apif_uri_scheme");
			activity_log_path = config.getString("activity_log_path");
			baseNS = config.getString("baseNS");
			repositoryURL = config.getString("repositoryURL");
			blazegraphPropertiesFilepath = config.getString("blazegraphPropertiesFilepath");
			baseResource = config.getString("baseResource");
			baseGraph = config.getString("baseGraph");
			ontologyURIPRefix = config.getString("ontologyURIPRefix");
			useNamedresources = config.getBoolean("useNamedresources");
			requestQueueSize = config.getInt("requestQueueSize");
			tmpFolder = config.getString("tmpFolder");
			lookupRateSeconds = config.getInt("lookupRateSeconds");
			blazegraphNamespacePrefix = config.getString("blazegraphNamespacePrefix");

		} catch (ConfigurationException e) {
			e.printStackTrace();
		}
	}

	public static RDFUploaderConfiguration getInstance() {
		if (instance == null) {
			instance = new RDFUploaderConfiguration();
		}
		return instance;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getApif_host() {
		return apif_host;
	}

	public void setApif_host(String apif_host) {
		this.apif_host = apif_host;
	}

	public String getLastTimestampFile() {
		return lastTimestampFile;
	}

	public void setLastTimestampFile(String lastTimestampFile) {
		this.lastTimestampFile = lastTimestampFile;
	}

	public String getApif_uri_scheme() {
		return apif_uri_scheme;
	}

	public void setApif_uri_scheme(String apif_uri_scheme) {
		this.apif_uri_scheme = apif_uri_scheme;
	}

	public String getActivity_log_path() {
		return activity_log_path;
	}

	public void setActivity_log_path(String activity_log_path) {
		this.activity_log_path = activity_log_path;
	}

	public String getBaseNS() {
		return baseNS;
	}

	public void setBaseNS(String baseNS) {
		this.baseNS = baseNS;
	}

	public String getRepositoryURL() {
		return repositoryURL;
	}

	public void setRepositoryURL(String repositoryURL) {
		this.repositoryURL = repositoryURL;
	}

	public String getBlazegraphPropertiesFilepath() {
		return blazegraphPropertiesFilepath;
	}

	public void setBlazegraphPropertiesFilepath(String blazegraphPropertiesFilepath) {
		this.blazegraphPropertiesFilepath = blazegraphPropertiesFilepath;
	}

	public String getBaseResource() {
		return baseResource;
	}

	public void setBaseResource(String baseResource) {
		this.baseResource = baseResource;
	}

	public String getBaseGraph() {
		return baseGraph;
	}

	public void setBaseGraph(String baseGraph) {
		this.baseGraph = baseGraph;
	}

	public String getOntologyURIPRefix() {
		return ontologyURIPRefix;
	}

	public void setOntologyURIPRefix(String ontologyURIPRefix) {
		this.ontologyURIPRefix = ontologyURIPRefix;
	}

	public boolean isUseNamedresources() {
		return useNamedresources;
	}

	public void setUseNamedresources(boolean useNamedresources) {
		this.useNamedresources = useNamedresources;
	}

	public int getRequestQueueSize() {
		return requestQueueSize;
	}

	public void setRequestQueueSize(int requestQueueSize) {
		this.requestQueueSize = requestQueueSize;
	}

	public String getTmpFolder() {
		return tmpFolder;
	}

	public void setTmpFolder(String tmpFolder) {
		this.tmpFolder = tmpFolder;
	}

	public int getLookupRateSeconds() {
		return lookupRateSeconds;
	}

	public void setLookupRateSeconds(int lookupRateSeconds) {
		this.lookupRateSeconds = lookupRateSeconds;
	}

	public String getBlazegraphNamespacePrefix() {
		return blazegraphNamespacePrefix;
	}

	public void setBlazegraphNamespacePrefix(String blazegraphNamespacePrefix) {
		this.blazegraphNamespacePrefix = blazegraphNamespacePrefix;
	}

}
