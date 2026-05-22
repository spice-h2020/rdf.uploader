package eu.spice.uploaders.rdfuploader.model;

import org.apache.jena.rdf.model.Model;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.spice.rdfuploader.RDFUploaderContext;
import eu.spice.rdfuploader.uploaders.Utils;

public class JSONRequestCreate implements Request {

	private JSONObject payload;
	private String dataset, docId;
	private RDFUploaderContext context;
	private static final Logger logger = LoggerFactory.getLogger(JSONRequestCreate.class);
	private boolean accomplished = false;

	public JSONRequestCreate(String dataset, String docId, JSONObject payload, RDFUploaderContext context) {
		this.payload = payload;
		this.dataset = dataset;
		this.docId = docId;
		this.context = context;
	}

	public JSONObject getPayload() {
		return payload;
	}

	public void setPayload(JSONObject payload) {
		this.payload = payload;
	}

	public String getDataset() {
		return dataset;
	}

	public void setDataset(String dataset) {
		this.dataset = dataset;
	}

	public void accomplishRequest() throws Exception {
		String docIdClean = basicEscaper.escape(docId);
		String root = context.getRootURI(dataset, docIdClean);
		String graphURI = context.getGraphURI(dataset, docIdClean);
		Model m = Utils.readOrTriplifyJSONObject(payload, root);
		logger.trace("Triplified Model contains {} triples", m.size());
		if (logger.isTraceEnabled()) {
			m.write(System.out, "TTL");
		}
		context.getTripleStoreClient().uploadModel(m, getTargetNamespace(), graphURI, false);
		logger.trace("Create Graph Request - Accomplished");
		accomplished = true;
	}

	@Override
	public String getDocId() {
		return docId;
	}

	public String getTargetNamespace() {
		return context.getNamespace(dataset);
	}

	@Override
	public boolean isAccomplished() {
		return accomplished;
	}

}
