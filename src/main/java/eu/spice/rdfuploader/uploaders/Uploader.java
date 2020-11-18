package eu.spice.rdfuploader.uploaders;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.StringReader;
import java.util.concurrent.BlockingQueue;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.rio.RDFFormat;

import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;
import com.github.spiceh2020.json2rdf.transformers.JSONTransformer;

import eu.spice.uploaders.rdfuploader.model.CreateNamespaceRequest;
import eu.spice.uploaders.rdfuploader.model.JSONRequestCreate;
import eu.spice.uploaders.rdfuploader.model.JSONRequestDelete;
import eu.spice.uploaders.rdfuploader.model.JSONRequestUpdate;
import eu.spice.uploaders.rdfuploader.model.Request;
import it.cnr.istc.stlab.lgu.commons.semanticweb.iterators.IteratorQuadFromTripleIterator;

public class Uploader implements Runnable {

	private BlockingQueue<Request> requests;
	private boolean stop = false;
	private String tmpFolder;
	private static Logger logger = LogManager.getLogger(Uploader.class);

	public Uploader(BlockingQueue<Request> uploadRequests, String tmpFolder) {
		super();
		this.requests = uploadRequests;
		this.tmpFolder = tmpFolder;
	}

	public void run() {
		logger.trace("Start uploader");
		while (!stop || !this.requests.isEmpty()) {
			try {
				logger.trace("Waiting for requests..");
				Request request = requests.take();
				logger.trace("Got request");
				if (request instanceof JSONRequestCreate) {
					accomplishRequest((JSONRequestCreate) request);
				} else if (request instanceof CreateNamespaceRequest) {
					accomplishRequest((CreateNamespaceRequest) request);
				} else if (request instanceof JSONRequestUpdate) {
					accomplishRequest((JSONRequestUpdate) request);
				} else if (request instanceof JSONRequestDelete) {
					accomplishRequest((JSONRequestDelete) request);
				}
				logger.trace("Request accomplished");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		logger.trace("Stopping uploader");
	}

	public void stop() {
		stop = true;
	}

	private void accomplishRequest(JSONRequestCreate r) throws Exception {
		logger.trace("Create Dataset Request");
		JSONTransformer jt = new JSONTransformer(r.getOntologyURIPrefix());
		if (r.getRootResourceURI() != null) {
			logger.trace("Setting root URI");
			jt.setURIRoot(r.getRootResourceURI());
		}
		RemoteRepositoryManager manager = new RemoteRepositoryManager(r.getRepositoryURL());
		RemoteRepository rr = Utils.createAndGetRemoteRepositoryForNamespace(manager, r.getNamespace(),
				r.getNamespaceProperties());

		Model m = ModelFactory.createDefaultModel();
		logger.trace("Reading as JSON-LD");
		RDFDataMgr.read(m, new StringReader(r.getPayload().toString()), "", Lang.JSONLD);
		logger.trace("Read " + m.size() + " triples from JSON-LD format!");
		if (m.size() == 0) {
			logger.trace("Trying to transform JSON document to RDF.");
			m = jt.getModel(r.getPayload());
			logger.trace("Read " + m.size() + " triples from JSON!");
		}

		String rdfFile = tmpFolder + "/" + System.nanoTime() + ".rdf";
		m.write(new FileOutputStream(new File(rdfFile)));
		if (r.getGraphURI() != null) {
			String nqFile = tmpFolder + "/" + System.nanoTime() + ".nq";
			RDFDataMgr.writeQuads(new FileOutputStream(new File(nqFile)),
					new IteratorQuadFromTripleIterator(
							RDFDataMgr.createIteratorTriples(new FileInputStream(new File(rdfFile)), Lang.RDFXML, ""),
							r.getGraphURI()));
			rr.add(new RemoteRepository.AddOp(new File(nqFile), RDFFormat.NQUADS));
			new File(nqFile).delete();
		} else {
			rr.add(new RemoteRepository.AddOp(new FileInputStream(new File(rdfFile)), RDFFormat.RDFXML));
		}
		manager.close();
		m.close();
		new File(rdfFile).delete();
		logger.trace("Create Dataset Request - Accomplished");
	}

	private void accomplishRequest(JSONRequestUpdate r) throws Exception {
		logger.trace("Update Dataset Request");
		JSONTransformer jt = new JSONTransformer(r.getOntologyURIPrefix());
		if (r.getRootResourceURI() != null) {
			logger.trace("Setting root URI");
			jt.setURIRoot(r.getRootResourceURI());
		}
		RemoteRepositoryManager manager = new RemoteRepositoryManager(r.getRepositoryURL());
		RemoteRepository rr = Utils.createAndGetRemoteRepositoryForNamespace(manager, r.getNamespace(),
				r.getNamespaceProperties());

		Model m = ModelFactory.createDefaultModel();
		logger.trace("Reading as JSON-LD");
		RDFDataMgr.read(m, new StringReader(r.getPayload().toString()), "", Lang.JSONLD);
		logger.trace("Read " + m.size() + " triples from JSON-LD format!");
		if (m.size() == 0) {
			logger.trace("Trying to transform JSON document to RDF.");
			m = jt.getModel(r.getPayload());
			logger.trace("Read " + m.size() + " triples from JSON!");
		}

		String rdfFile = tmpFolder + "/" + System.nanoTime() + ".rdf";
		m.write(new FileOutputStream(new File(rdfFile)));
		if (r.getGraphURI() != null) {
			String nqFile = tmpFolder + "/" + System.nanoTime() + ".nq";
			RDFDataMgr.writeQuads(new FileOutputStream(new File(nqFile)),
					new IteratorQuadFromTripleIterator(
							RDFDataMgr.createIteratorTriples(new FileInputStream(new File(rdfFile)), Lang.RDFXML, ""),
							r.getGraphURI()));

			rr.prepareUpdate("CLEAR GRAPH <" + r.getGraphURI() + ">").evaluate();
			rr.add(new RemoteRepository.AddOp(new File(nqFile), RDFFormat.NQUADS));
			new File(nqFile).delete();

		} else {
			rr.add(new RemoteRepository.AddOp(new FileInputStream(new File(rdfFile)), RDFFormat.RDFXML));
		}
		manager.close();
		m.close();
		logger.trace("Update Dataset Request - Accomplished");
		new File(rdfFile).delete();
	}

	private void accomplishRequest(JSONRequestDelete r) throws Exception {
		logger.trace("CLEAR graph " + r.getGraphURI());
		RemoteRepositoryManager manager = new RemoteRepositoryManager(r.getRepositoryURL());
		RemoteRepository rr = Utils.createAndGetRemoteRepositoryForNamespace(manager, r.getNamespace(),
				r.getNamespaceProperties());
		rr.prepareUpdate("CLEAR GRAPH <" + r.getGraphURI() + ">").evaluate();
		logger.trace("CLEAR graph " + r.getGraphURI() + " completed!");
		manager.close();
	}

	private void accomplishRequest(CreateNamespaceRequest r) throws Exception {
		logger.trace("Create Namespace Request");
		RemoteRepositoryManager manager = new RemoteRepositoryManager(r.getRepositoryURL());
		Utils.createAndGetRemoteRepositoryForNamespace(manager, r.getNamespace(), r.getNamespaceProperties());
		manager.close();
		logger.trace("Create Namespace Request.. accomplished");
	}

}
