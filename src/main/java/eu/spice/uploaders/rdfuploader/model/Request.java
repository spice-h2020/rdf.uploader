package eu.spice.uploaders.rdfuploader.model;

public interface Request {

	public String getDocId();

	public void accomplishRequest() throws Exception;

}
