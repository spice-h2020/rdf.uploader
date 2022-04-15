
package eu.spice.uploaders.rdfuploader.model;

import java.io.File;
import java.util.Properties;

import org.apache.jena.rdf.model.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.spice.rdfuploader.RDFUploaderContext;
import eu.spice.rdfuploader.uploaders.Utils;

public class CreateFileRequest implements Request {

	private String filename, dataset;
	private RDFUploaderContext context;
	private static final Logger logger = LoggerFactory.getLogger(ConstructRequest.class);
	private boolean accomplished = false;

	public CreateFileRequest(String filename, String dataset, RDFUploaderContext context) {
		super();
		this.filename = filename;
		this.dataset = dataset;
		this.context = context;
	}

	@Override
	public String getDocId() {
		return null;
	}

	@Override
	public void accomplishRequest() throws Exception {
		String filenameClean = basicEscaper.escape(filename);
		String root = context.getRootURI(dataset, filenameClean);
//		String ontologyPrefix = context.getOntologyURIPrefix(dataset, filename);
		String graphURI = context.getGraphURI(dataset, filenameClean);
		Properties namespaceProperties = Utils.loadProperties(context.getConf().getBlazegraphPropertiesFilepath());
		String namespace = context.getBlazegraphNamespace(dataset);

		// Downloading file
		File tempFolderDataset = Utils.makeTempFolderForDataset(context.getConf().getTmpFolder(), dataset);
		File downloadedFile = new File(tempFolderDataset, filename);
		context.getDbClient().downloadFile(dataset, filename, downloadedFile);

		// Triplify file
		Model m = context.getSPARQLAnythingClient().triplifyFile(downloadedFile.getAbsolutePath(), root);

		// Upload file
		context.getBlazegraphClient().uploadModel(m, namespace, graphURI, namespaceProperties, true);

		logger.trace("File {} uploaded to namespace {} graphURI {}", filename, namespace, graphURI);
		boolean r = downloadedFile.delete();
		logger.trace("File {} deleted {}?", filename, r);
		accomplished = true;
	}

	@Override
	public boolean isAccomplished() {
		return accomplished;
	}

	@Override
	public String getDataset() {
		return dataset;
	}

}
