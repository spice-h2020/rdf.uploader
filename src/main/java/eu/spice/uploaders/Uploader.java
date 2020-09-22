package eu.spice.uploaders;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.concurrent.BlockingQueue;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.rio.RDFFormat;

import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;

import eu.spice.json2rdf.transformers.JSONTransformer;
import eu.spice.uploaders.model.JSONRequest;
import eu.spice.uploaders.model.Request;
import it.cnr.istc.stlab.lgu.commons.semanticweb.iterators.IteratorQuadFromTripleIterator;

public class Uploader implements Runnable {

	private BlockingQueue<Request> uploadRequests;
	private boolean stop = false;
	private String tmpFolder;
	private static Logger logger = LogManager.getLogger(Uploader.class);

	public Uploader(BlockingQueue<Request> uploadRequests, String tmpFolder) {
		super();
		this.uploadRequests = uploadRequests;
		this.tmpFolder = tmpFolder;
	}

	public void run() {
		logger.trace("Start uploader");
		while (!stop || !uploadRequests.isEmpty()) {
			try {
				logger.trace("Waiting for requests..");
				Request request = uploadRequests.take();
				logger.trace("Got request");
				if (request instanceof JSONRequest) {
					accomplishRequest((JSONRequest) request);
				}
				logger.trace("Request accomplished");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public void stop() {
		stop = true;
	}

	private void accomplishRequest(JSONRequest r) throws Exception {
		JSONTransformer jt = new JSONTransformer(r.getOntologyURIPrefix());
		if (r.getResourceURIPrefix() != null) {
			jt.setResourcePrefix(r.getResourceURIPrefix());
		}
		RemoteRepositoryManager manager = new RemoteRepositoryManager(r.getRepositoryURL());

		if (!Utils.namespaceExists(manager, r.getNamespace())) {
			manager.createRepository(r.getNamespace(), r.getNamespaceProperties());
		} else {
			logger.trace("Namespace " + r.getNamespace() + " already exists!");
		}
		RemoteRepository rr = manager.getRepositoryForNamespace(r.getNamespace());
		Model m = jt.getModel(r.getPayload());
		String rdfFile = tmpFolder + "/" + System.nanoTime() + ".rdf";
		m.write(new FileOutputStream(new File(rdfFile)));
		if (r.getGraphURI() != null) {
			String nqFile = tmpFolder + "/" + System.nanoTime() + ".nq";
			RDFDataMgr.writeQuads(new FileOutputStream(new File(nqFile)),
					new IteratorQuadFromTripleIterator(
							RDFDataMgr.createIteratorTriples(new FileInputStream(new File(rdfFile)), Lang.RDFXML, ""),
							r.getGraphURI()));
			rr.add(new RemoteRepository.AddOp(new File(nqFile), RDFFormat.NQUADS));
		} else {
			rr.add(new RemoteRepository.AddOp(new FileInputStream(new File(rdfFile)), RDFFormat.RDFXML));
		}
		manager.close();
		m.close();
	}

}
