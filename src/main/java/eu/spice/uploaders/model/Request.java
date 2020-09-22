package eu.spice.uploaders.model;

import java.util.Properties;

public interface Request {

	public String getNamespace();

	public String getRepositoryURL();

	public String getGraphURI();

	public Properties getNamespaceProperties();

	public Object getPayload();

}
