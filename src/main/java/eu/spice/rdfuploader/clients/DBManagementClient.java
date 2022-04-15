
package eu.spice.rdfuploader.clients;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.openjson.JSONArray;

public class DBManagementClient {

	private static final Logger logger = LoggerFactory.getLogger(DBManagementClient.class);
	private String username, password, apif_uri_scheme, apif_host;
	public static final int TIMEOUT = 10000;

	public DBManagementClient(String username, String password, String apif_uri_scheme, String apif_host) {
		super();
		this.username = username;
		this.password = password;
		this.apif_uri_scheme = apif_uri_scheme;
		this.apif_host = apif_host;
	}

	public void createDataset(String datasetId, String key) {
		CredentialsProvider provider = new BasicCredentialsProvider();
		UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(username, password);
		provider.setCredentials(AuthScope.ANY, credentials);
		HttpClient client = HttpClientBuilder.create().setDefaultCredentialsProvider(provider).build();

		RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(TIMEOUT).setConnectTimeout(TIMEOUT)
				.setConnectionRequestTimeout(TIMEOUT).build();

		URIBuilder builder = new URIBuilder();
		builder.setScheme(apif_uri_scheme).setHost(apif_host).setPath("/management/datasets");

		HttpPost postRequest = new HttpPost();

		try {
			logger.trace(builder.build().toString());
			postRequest.setURI(builder.build());
			postRequest.addHeader("Content-Type", "application/x-www-form-urlencoded");
			postRequest.setConfig(requestConfig);
			postRequest.setEntity(new StringEntity(String.format("dataset-uuid=%s&key=%s", datasetId, key)));
			HttpHost host = new HttpHost(apif_host);
			HttpResponse response = client.execute(host, postRequest);
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

	public Set<String> getDatasets() {
		CredentialsProvider provider = new BasicCredentialsProvider();
		UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(username, password);
		provider.setCredentials(AuthScope.ANY, credentials);
		HttpClient client = HttpClientBuilder.create().setDefaultCredentialsProvider(provider).build();

		RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(TIMEOUT).setConnectTimeout(TIMEOUT)
				.setConnectionRequestTimeout(TIMEOUT).build();

		URIBuilder builder = new URIBuilder();
		builder.setScheme(apif_uri_scheme).setHost(apif_host).setPath("/management/datasets");

		HttpGet deleteRequest = new HttpGet();

		try {
			logger.trace(builder.build().toString());
			deleteRequest.setURI(builder.build());
			deleteRequest.addHeader("Content-Type", "application/x-www-form-urlencoded");
			deleteRequest.setConfig(requestConfig);
			HttpHost host = new HttpHost(apif_host);
			HttpResponse response = client.execute(host, deleteRequest);
			final HttpEntity resEntity = response.getEntity();
			if (resEntity != null) {
				logger.trace("Response content length: " + resEntity.getContentLength());
				logger.trace(resEntity.toString());
				String resultString = new String(EntityUtils.toByteArray(resEntity));
				logger.trace(resultString);
				JSONArray a = new JSONArray(resultString);
				Set<String> result = new HashSet<>();
				for (int i = 0; i < a.length(); i++) {
					result.add(a.getString(i));
				}
				return result;
			}
		} catch (IOException e) {
			logger.error(e.getMessage());
		} catch (URISyntaxException e) {
			logger.error(e.getMessage());
		}
		return null;

	}

}
