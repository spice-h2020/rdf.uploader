package eu.spice.uploaders.rdfuploader.model;

public interface Request {

	public String getTargetNamespace();
	
	public String getDocId();

	public String getRepositoryURL();

	public void accomplishRequest() throws Exception;

}
