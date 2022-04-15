package eu.spice.uploaders.rdfuploader.model;

import com.google.common.escape.UnicodeEscaper;
import com.google.common.net.PercentEscaper;

public interface Request {

	public static UnicodeEscaper basicEscaper = new PercentEscaper("%", false);

	public String getDocId();
	
	public String getDataset();

	public void accomplishRequest() throws Exception;
	
	public boolean isAccomplished();

}
