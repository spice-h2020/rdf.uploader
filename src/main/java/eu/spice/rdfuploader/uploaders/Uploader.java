package eu.spice.rdfuploader.uploaders;

import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.spice.uploaders.rdfuploader.model.Request;

public class Uploader implements Runnable {

	private BlockingQueue<Request> requests;
	private boolean stop = false;
	private static final Logger logger = LoggerFactory.getLogger(Uploader.class);

	public Uploader(BlockingQueue<Request> uploadRequests, String tmpFolder) {
		super();
		this.requests = uploadRequests;
	}

	public void run() {
		logger.trace("Start uploader");
		while (!stop || !this.requests.isEmpty()) {
			try {
				logger.trace("Waiting for requests..");
				Request request = requests.take();
				logger.trace("Got request");
//				if (request instanceof JSONRequestCreate) {
////					accomplishRequest((JSONRequestCreate) request);
//				} else if (request instanceof CreateNamespaceRequest) {
////					accomplishRequest((CreateNamespaceRequest) request);
//				} else if (request instanceof JSONRequestUpdate) {
////					accomplishRequest((JSONRequestUpdate) request);
//				} else if (request instanceof JSONRequestDelete) {
//					accomplishRequest((JSONRequestDelete) request);
//				} else if (request instanceof ConstructKnowledgeGraph) {
//					accomplishRequest((ConstructKnowledgeGraph) request);
//				}
				request.accomplishRequest();
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

//	private void accomplishRequest(JSONRequestCreate r) throws Exception {
//		logger.debug("Create Dataset Request");
//		JSONTriplifier jt = new JSONTriplifier();
////		JSONTransformer jt = new JSONTransformer(r.getOntologyURIPrefix());
//		Properties p = new Properties();
//		p.setProperty(IRIArgument.CONTENT.toString(), r.getPayload().toString());
//		p.setProperty(IRIArgument.NAMESPACE.toString(), r.getOntologyURIPrefix());
//		if (r.getRootResourceURI() != null) {
//			logger.trace("Setting root URI {}", r.getRootResourceURI());
////			jt.setURIRoot(r.getRootResourceURI());
//			p.setProperty(IRIArgument.BLANK_NODES.toString(), "false");
//			p.setProperty(IRIArgument.ROOT.toString(), r.getRootResourceURI());
//		}
//		RemoteRepositoryManager manager = new RemoteRepositoryManager(r.getRepositoryURL());
//		RemoteRepository rr = Utils.createAndGetRemoteRepositoryForNamespace(manager, r.getTargetNamespace(),
//				r.getNamespaceProperties());
//
//		Model m = ModelFactory.createDefaultModel();
//		logger.trace("Reading file {}", r.getPayload().toString());
//		RDFDataMgr.read(m, new StringReader(r.getPayload().toString()), "", Lang.JSONLD);
//		logger.trace("Read " + m.size() + " triples from JSON-LD format!");
//		if (m.size() == 0) {
//			logger.trace("Trying to transform JSON document to RDF.");
////			m = jt.getModel(r.getPayload());
//			m = ModelFactory
//					.createModelForGraph(jt.triplify(p, new BaseFacadeXBuilder("uploader", p)).getDefaultGraph());
//			logger.trace("Read " + m.size() + " triples from JSON!");
//		}
//
//		String rdfFile = tmpFolder + "/" + System.nanoTime() + ".nt";
//		m.write(new FileOutputStream(new File(rdfFile)), "NT");
//		if (r.getGraphURI() != null) {
//			String nqFile = tmpFolder + "/" + System.nanoTime() + ".nq";
//			RDFDataMgr.writeQuads(new FileOutputStream(new File(nqFile)),
//					new IteratorQuadFromTripleIterator(
//							RDFDataMgr.createIteratorTriples(new FileInputStream(new File(rdfFile)), Lang.NT, ""),
//							r.getGraphURI()));
//			rr.add(new RemoteRepository.AddOp(new File(nqFile), RDFFormat.NQUADS));
//			new File(nqFile).delete();
//		} else {
//			rr.add(new RemoteRepository.AddOp(new FileInputStream(new File(rdfFile)), RDFFormat.NTRIPLES));
//		}
//		manager.close();
//		m.close();
//		new File(rdfFile).delete();
//		logger.trace("Create Dataset Request - Accomplished");
//	}

//	private void accomplishRequest(JSONRequestUpdate r) throws Exception {
//		logger.debug("Update Dataset Request");
//		JSONTriplifier jt = new JSONTriplifier();
////		JSONTransformer jt = new JSONTransformer(r.getOntologyURIPrefix());
//		Properties p = new Properties();
//		p.setProperty(IRIArgument.CONTENT.toString(), r.getPayload().toString());
//		p.setProperty(IRIArgument.NAMESPACE.toString(), r.getOntologyURIPrefix());
//		if (r.getRootResourceURI() != null) {
//			logger.trace("Setting root URI {}", r.getRootResourceURI());
////			jt.setURIRoot(r.getRootResourceURI());
//			p.setProperty(IRIArgument.BLANK_NODES.toString(), "false");
//			p.setProperty(IRIArgument.ROOT.toString(), r.getRootResourceURI());
//
//		}
//		RemoteRepositoryManager manager = new RemoteRepositoryManager(r.getRepositoryURL());
//		RemoteRepository rr = Utils.createAndGetRemoteRepositoryForNamespace(manager, r.getTargetNamespace(),
//				r.getNamespaceProperties());
//
//		Model m = ModelFactory.createDefaultModel();
//		logger.trace("Reading as JSON-LD");
//		RDFDataMgr.read(m, new StringReader(r.getPayload().toString()), "", Lang.JSONLD);
//		logger.trace("Read " + m.size() + " triples from JSON-LD format!");
//		if (m.size() == 0) {
//			logger.trace("Trying to transform JSON document to RDF.");
////			m = jt.getModel(r.getPayload());
////			jt.triplify(p, new BaseFacadeXBuilder("uploader", p));
//			m = ModelFactory
//					.createModelForGraph(jt.triplify(p, new BaseFacadeXBuilder("uploader", p)).getDefaultGraph());
//			logger.trace("Read " + m.size() + " triples from JSON!");
//		}
//
//		String rdfFile = tmpFolder + "/" + System.nanoTime() + ".nt";
//		m.write(new FileOutputStream(new File(rdfFile)), "NT");
//		if (r.getGraphURI() != null) {
//			String nqFile = tmpFolder + "/" + System.nanoTime() + ".nq";
//			RDFDataMgr.writeQuads(new FileOutputStream(new File(nqFile)),
//					new IteratorQuadFromTripleIterator(
//							RDFDataMgr.createIteratorTriples(new FileInputStream(new File(rdfFile)), Lang.NT, ""),
//							r.getGraphURI()));
//
//			rr.prepareUpdate("CLEAR GRAPH <" + r.getGraphURI() + ">").evaluate();
//			rr.add(new RemoteRepository.AddOp(new File(nqFile), RDFFormat.NQUADS));
//			new File(nqFile).delete();
//
//		} else {
//			rr.add(new RemoteRepository.AddOp(new FileInputStream(new File(rdfFile)), RDFFormat.NTRIPLES));
//		}
//		manager.close();
//		m.close();
//		logger.trace("Update Dataset Request - Accomplished");
//		new File(rdfFile).delete();
//	}

//	private void accomplishRequest(JSONRequestDelete r) throws Exception {
//		logger.debug("CLEAR graph " + r.getGraphURI());
//		RemoteRepositoryManager manager = new RemoteRepositoryManager(r.getRepositoryURL());
//		RemoteRepository rr = Utils.createAndGetRemoteRepositoryForNamespace(manager, r.getTargetNamespace(),
//				r.getNamespaceProperties());
//		rr.prepareUpdate("CLEAR GRAPH <" + r.getGraphURI() + ">").evaluate();
//		logger.trace("CLEAR graph " + r.getGraphURI() + " completed!");
//		manager.close();
//	}

//	private void accomplishRequest(CreateNamespaceRequest r) throws Exception {
//		logger.debug("Create Namespace Request");
//		RemoteRepositoryManager manager = new RemoteRepositoryManager(r.getRepositoryURL());
//		Utils.createAndGetRemoteRepositoryForNamespace(manager, r.getTargetNamespace(), r.getNamespaceProperties());
//		manager.close();
//		logger.trace("Create Namespace Request.. accomplished");
//	}

}
