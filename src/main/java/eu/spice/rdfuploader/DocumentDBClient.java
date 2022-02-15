
package eu.spice.rdfuploader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URISyntaxException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DocumentDBClient {

	private static final Logger logger = LoggerFactory.getLogger(DocumentDBClient.class);
	private String username, password, apif_uri_scheme, apif_host, activity_log_path, baseNS;
	public static final int TIMEOUT = 10000;

	public DocumentDBClient(String username, String password, String apif_uri_scheme, String apif_host,
			String activity_log_path, String baseNS) {
		super();
		this.username = username;
		this.password = password;
		this.apif_uri_scheme = apif_uri_scheme;
		this.apif_host = apif_host;
		this.activity_log_path = activity_log_path;
		this.baseNS = baseNS;
	}

	public Model getActivityLogEntitiesFromBrowse(Integer lastTimestamp) {
		logger.trace("Method getActivityLogEntitiesFromBrowse invoked");
		Model m = ModelFactory.createDefaultModel();

		CredentialsProvider provider = new BasicCredentialsProvider();
		UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(username, password);
		provider.setCredentials(AuthScope.ANY, credentials);
		HttpClient client = HttpClientBuilder.create().setDefaultCredentialsProvider(provider).build();

		logger.trace("HTTP client ready - auth configured");
		RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(TIMEOUT).setConnectTimeout(TIMEOUT)
				.setConnectionRequestTimeout(TIMEOUT).build();
		HttpResponse response;
		try {

			URIBuilder builder = new URIBuilder();
			builder.setScheme(apif_uri_scheme).setHost(apif_host).setPath(activity_log_path);

			if (lastTimestamp != null) {
				builder.setParameter("query", "{ \"_timestamp\": {  \"$gt\":" + lastTimestamp
						+ "  }, \"$or\": [ {\"@type\":\"al:Create\"}, {\"@type\":\"al:Update\"}, {\"@type\":\"al:Delete\"}, {\"@type\":\"al:CreateDataset\"}]}");
			} else {
				builder.setParameter("query",
						"{ \"$or\": [ {\"@type\":\"al:Create\"}, {\"@type\":\"al:Update\"}, {\"@type\":\"al:Delete\"}, {\"@type\":\"al:CreateDataset\"}]}");
			}

			builder.setParameter("pagesize", "100");

			int pageNumber = 1;
			logger.trace("Start browsing");
			while (true) {

				logger.debug("Calling page number {}", pageNumber);

				builder.setParameter("page", pageNumber + "");
				HttpGet getRequest = new HttpGet(builder.build());
				getRequest.setConfig(requestConfig);
				response = client.execute(getRequest);
				logger.debug("Response: {}", response.getStatusLine().toString());
				BufferedReader br = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));

				String l;
				StringBuilder sb = new StringBuilder();
				while ((l = br.readLine()) != null) {
					sb.append(l);
				}

				JSONObject objectResponse = new JSONObject(sb.toString());
				if (objectResponse.has("error")) {
					logger.error(objectResponse.getString("error"));
				}
				logger.trace("Response content");
				JSONArray results = objectResponse.getJSONArray("results");

				logger.debug("Document  count " + objectResponse.getInt("documentCount") + " Dimension results "
						+ results.length());
				if (results.length() > 0) {
					RDFDataMgr.read(m, new StringReader(results.toString()), baseNS, Lang.JSONLD);
				} else {
					break;
				}
				pageNumber++;
			}

		} catch (IOException | URISyntaxException e) {
			logger.error("{}", e.toString());
		}

		return m;

	}

	public void updateDocument(String datasetId, String documentId, JSONObject newDocument) {
		logger.trace("Method updateDocument invoked");

		CredentialsProvider provider = new BasicCredentialsProvider();
		UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(username, password);
		provider.setCredentials(AuthScope.ANY, credentials);
		HttpClient client = HttpClientBuilder.create().setDefaultCredentialsProvider(provider).build();

		logger.trace("HTTP client ready - auth configured");
		RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(TIMEOUT).setConnectTimeout(TIMEOUT)
				.setConnectionRequestTimeout(TIMEOUT).build();

		URIBuilder builder = new URIBuilder();
		builder.setScheme(apif_uri_scheme).setHost(apif_host).setPath("/object/" + datasetId + "/" + documentId);

		HttpPut putRequest = new HttpPut();

		try {
			putRequest.setURI(builder.build());
			putRequest.addHeader("Content-Type", "application/json");
			putRequest.setConfig(requestConfig);
			putRequest.setEntity(new StringEntity(newDocument.toString()));
			HttpHost host = new HttpHost(apif_host);
			HttpResponse response = client.execute(host, putRequest);
			final HttpEntity resEntity = response.getEntity();
			if (resEntity != null) {
				logger.trace("Response content length: " + resEntity.getContentLength());
				logger.trace(resEntity.toString());
				logger.trace(new String(EntityUtils.toByteArray(resEntity)));
			}
		} catch (IOException e) {
			logger.error(e.getMessage());
		} catch (URISyntaxException e) {
			logger.error(e.getMessage());
		}

	}

	public JSONObject retrieveDocument(String datasetId, String documentId) {
		logger.trace("Method retrieveDocument invoked");

		CredentialsProvider provider = new BasicCredentialsProvider();
		UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(username, password);
		provider.setCredentials(AuthScope.ANY, credentials);
		HttpClient client = HttpClientBuilder.create().setDefaultCredentialsProvider(provider).build();

		logger.trace("HTTP client ready - auth configured");
		RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(TIMEOUT).setConnectTimeout(TIMEOUT)
				.setConnectionRequestTimeout(TIMEOUT).build();

		URIBuilder builder = new URIBuilder();
		builder.setScheme(apif_uri_scheme).setHost(apif_host).setPath("/object/" + datasetId + "/" + documentId);

		HttpGet getRequest = new HttpGet();

		try {
			getRequest.setURI(builder.build());
			getRequest.addHeader("Content-Type", "application/json");
			getRequest.setConfig(requestConfig);
			HttpHost host = new HttpHost(apif_host);
			HttpResponse response = client.execute(host, getRequest);
			final HttpEntity resEntity = response.getEntity();
			if (resEntity != null) {
				logger.trace("Response content length: " + resEntity.getContentLength());
				String objString = new String(EntityUtils.toByteArray(resEntity));
				logger.trace("Result {}", objString);
				return new JSONArray(objString).getJSONObject(0);
			}
			return null;
		} catch (IOException e) {
			logger.error(e.getMessage());
		} catch (URISyntaxException e) {
			logger.error(e.getMessage());
		}
		return null;

	}

	public JSONArray retrieveDocuments(String datasetId)
			throws URISyntaxException, ClientProtocolException, IOException {
		logger.trace("Method retrieveDocument invoked");

		CredentialsProvider provider = new BasicCredentialsProvider();
		UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(username, password);
		provider.setCredentials(AuthScope.ANY, credentials);
		HttpClient client = HttpClientBuilder.create().setDefaultCredentialsProvider(provider).build();

		logger.trace("HTTP client ready - auth configured");
		RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(TIMEOUT).setConnectTimeout(TIMEOUT)
				.setConnectionRequestTimeout(TIMEOUT).build();

		URIBuilder builder = new URIBuilder();
		builder.setScheme(apif_uri_scheme).setHost(apif_host).setPath("/browse/" + datasetId);
		builder.setParameter("pagesize", "100");

		HttpResponse response;
		JSONArray result = new JSONArray();

		int pageNumber = 1;
		logger.trace("Start browsing");
		while (true) {

			logger.debug("Calling page number {}", pageNumber);

			builder.setParameter("page", String.valueOf(pageNumber));
			HttpGet getRequest = new HttpGet(builder.build());
			getRequest.addHeader("Content-Type", "application/json");
			getRequest.setConfig(requestConfig);
			response = client.execute(getRequest);
			logger.debug("Response: {}", response.getStatusLine().toString());

			final HttpEntity resEntity = response.getEntity();

			if (resEntity != null) {
				logger.trace("Response content length: " + resEntity.getContentLength());
				String objString = new String(EntityUtils.toByteArray(resEntity));
				logger.trace("Result {}", objString);
				JSONArray chunk = new JSONObject(objString).getJSONArray("results");
				chunk.forEach(o -> result.put(o));
				if (chunk.length() == 0) {
					break;
				}
			} else {
				break;
			}

			pageNumber++;
		}

		return result;

	}

}
