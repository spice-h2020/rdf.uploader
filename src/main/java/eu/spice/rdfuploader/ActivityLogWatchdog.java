package eu.spice.rdfuploader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;

import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import eu.spice.rdfuploader.uploaders.Utils;
import eu.spice.uploaders.rdfuploader.model.CreateNamespaceRequest;
import eu.spice.uploaders.rdfuploader.model.JSONRequestCreate;
import eu.spice.uploaders.rdfuploader.model.JSONRequestDelete;
import eu.spice.uploaders.rdfuploader.model.JSONRequestUpdate;
import eu.spice.uploaders.rdfuploader.model.Request;

public class ActivityLogWatchdog implements Runnable {

	private static final Logger logger = LogManager.getLogger(ActivityLogWatchdog.class);

	private String password, username, apif_host, lastTimestampFile, apif_uri_scheme, activity_log_path, baseNS,
			repositoryURL, blazegraphPropertiesFilepath, baseResource, baseGraph, ontologyURIPRefix;

	private boolean useNamedresources = true;

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

	public ActivityLogWatchdog(RDFUploaderConfiguration c, BlockingQueue<Request> requests) throws IOException {
		this.requests = requests;
		init(c);
	}

	private void init(RDFUploaderConfiguration c) throws IOException {
		blazegraphProperties = Utils.loadProperties(blazegraphPropertiesFilepath);
		username = c.getUsername();
		password = c.getPassword();
		apif_host = c.getApif_host();
		lastTimestampFile = c.getLastTimestampFile();
		apif_uri_scheme = c.getApif_uri_scheme();
		activity_log_path = c.getActivity_log_path();
		baseNS = c.getBaseNS();
		repositoryURL = c.getRepositoryURL();
		blazegraphPropertiesFilepath = c.getBlazegraphPropertiesFilepath();
		baseResource = c.getBaseResource();
		baseGraph = c.getBaseGraph();
		ontologyURIPRefix = c.getOntologyURIPRefix();
		useNamedresources = c.isUseNamedresources();
	}

	@Override
	public void run() {
		Integer lastTimestamp = null;
		try {
			lastTimestamp = getLastTimestamp();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		logger.trace("Last timestamp " + lastTimestamp);

		CredentialsProvider provider = new BasicCredentialsProvider();
		UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(username, password);
		provider.setCredentials(AuthScope.ANY, credentials);
		HttpClient client = HttpClientBuilder.create().setDefaultCredentialsProvider(provider).build();

		HttpResponse response;
		try {

			URIBuilder builder = new URIBuilder();
			builder.setScheme(apif_uri_scheme).setHost(apif_host).setPath(activity_log_path);

			if (lastTimestamp != null) {
				builder.setParameter("query", "{ \"_timestamp\": {  \"$gt\":" + lastTimestamp + "  }}");
			}

			response = client.execute(new HttpGet(builder.build()));
			BufferedReader br = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
			String l;
			StringBuilder sb = new StringBuilder();
			while ((l = br.readLine()) != null) {
				sb.append(l);
			}

			Model m = ModelFactory.createDefaultModel();
			RDFDataMgr.read(m, new StringReader(sb.toString()), baseNS, Lang.JSONLD);
			QueryExecution qexec = QueryExecutionFactory.create(getLastOperationsQuery, m);
			ResultSet rs = qexec.execSelect();
//			System.out.println(ResultSetFormatter.asText(rs));
			while (rs.hasNext()) {
				QuerySolution qs = rs.next();
				lastTimestamp = qs.get("timestamp").asLiteral().getInt();
				logger.trace("Processing " + qs.get("operationType").asResource().getLocalName() + " operation "
						+ qs.get("ale").asResource().getLocalName() + " with timestamp " + lastTimestamp);
				String datasetId = qs.get("datasetId").asLiteral().getString();

				Resource operationType = qs.get("operationType").asResource();
				if (operationType.equals(CREATE_DATASET)) {
					logger.trace("Create Dataset");
					this.requests.put(new CreateNamespaceRequest(datasetId, repositoryURL, blazegraphProperties));
				} else if (operationType.equals(CREATE)) {
					logger.trace("Create Document");
					String payload = qs.get("payload").asLiteral().getString();
					String docId = qs.get("docId").asLiteral().getString();
					JSONRequestCreate request = new JSONRequestCreate(datasetId, repositoryURL,
							getGraphURI(datasetId, docId), blazegraphProperties, new JSONObject(payload),
							getOntologyURIPrefix(datasetId, docId));
					if (useNamedresources) {
						logger.trace("Use named resources");
						request.setRootResourceURI(getRootURI(datasetId, docId));
					}
					this.requests.put(request);
				} else if (operationType.equals(DELETE)) {
					logger.trace("Delete Document");
					// TODO remove this as soon as issue 5 is addressed
					String endpoint = qs.get("endpoint").asLiteral().getString();
					String[] split = endpoint.split("/");
					String docId = split[split.length - 1];
					this.requests.put(new JSONRequestDelete(datasetId, repositoryURL, getGraphURI(datasetId, docId),
							blazegraphProperties));
					logger.trace(this.requests.size());
				} else if (operationType.equals(UPDATE)) {
					logger.trace("Update Document");
					String payload = qs.get("payload").asLiteral().getString();
					String docId = qs.get("docId").asLiteral().getString();
					JSONRequestUpdate request = new JSONRequestUpdate(datasetId, repositoryURL,
							getGraphURI(datasetId, docId), blazegraphProperties, new JSONObject(payload),
							getOntologyURIPrefix(datasetId, docId));
					if (useNamedresources) {
						logger.trace("Use named resources");
						request.setRootResourceURI(getRootURI(datasetId, docId));
					}
					this.requests.put(request);
				}

			}

			saveLastTimestamp(lastTimestamp);

		} catch (IOException | URISyntaxException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	private String getGraphURI(String datasetId, String docId) {
		return baseGraph + datasetId + "/" + docId;
	}

	private String getRootURI(String datasetId, String docId) {
		return baseResource + datasetId + "/" + docId;
	}

	private String getOntologyURIPrefix(String datasetId, String docId) {
		return ontologyURIPRefix + datasetId + "/" + docId + "/";
	}

	private Integer getLastTimestamp() throws IOException {
		File timestamp = new File(lastTimestampFile);
		if (timestamp.exists()) {
			BufferedReader br = new BufferedReader(new FileReader(timestamp));
			String line = br.readLine();
			br.close();
			if (line != null) {
				return Integer.parseInt(line);
			}
		}
		return null;
	}

	private void saveLastTimestamp(Integer timestamp) throws IOException {
		File timestampFile = new File(lastTimestampFile);
		FileOutputStream fos = new FileOutputStream(timestampFile);
		fos.write((timestamp + "\n").getBytes());
		fos.close();
	}

}
