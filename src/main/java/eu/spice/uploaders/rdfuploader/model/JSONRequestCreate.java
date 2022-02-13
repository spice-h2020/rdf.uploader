package eu.spice.uploaders.rdfuploader.model;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.StringReader;
import java.util.Properties;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.json.JSONObject;
import org.openrdf.rio.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;
import com.github.sparqlanything.json.JSONTriplifier;
import com.github.sparqlanything.model.BaseFacadeXBuilder;
import com.github.sparqlanything.model.IRIArgument;

import eu.spice.rdfuploader.RDFUploaderConfiguration;
import eu.spice.rdfuploader.RDFUploaderContext;
import it.cnr.istc.stlab.lgu.commons.semanticweb.iterators.IteratorQuadFromTripleIterator;

public class JSONRequestCreate extends JSONRequest {

	private JSONObject payload;
	private String ontologyURIPrefix, rootResourceURI;
	private static final Logger logger = LoggerFactory.getLogger(JSONRequestCreate.class);

	public JSONRequestCreate(String namespace, String repositoryURL, String graphURI, Properties namespaceProperties,
			JSONObject payload, String ontologyURIPrefix, RDFUploaderContext context) {
		super(namespace, repositoryURL, graphURI, namespaceProperties, context);
		this.payload = payload;
		this.ontologyURIPrefix = ontologyURIPrefix;
	}

	public JSONObject getPayload() {
		return payload;
	}

	public void setPayload(JSONObject payload) {
		this.payload = payload;
	}

	public String getRootResourceURI() {
		return rootResourceURI;
	}

	public void setRootResourceURI(String resourceURIPrefix) {
		this.rootResourceURI = resourceURIPrefix;
	}

	public String getOntologyURIPrefix() {
		return ontologyURIPrefix;
	}

	public void setOntologyURIPrefix(String ontologyURIPrefix) {
		this.ontologyURIPrefix = ontologyURIPrefix;
	}

	public void accomplishRequest() throws Exception {
		logger.debug("Create Dataset Request");
		JSONTriplifier jt = new JSONTriplifier();
//		JSONTransformer jt = new JSONTransformer(r.getOntologyURIPrefix());
		Properties p = new Properties();
		p.setProperty(IRIArgument.CONTENT.toString(), this.getPayload().toString());
		p.setProperty(IRIArgument.NAMESPACE.toString(), this.getOntologyURIPrefix());
		if (this.getRootResourceURI() != null) {
			logger.trace("Setting root URI {}", this.getRootResourceURI());
//			jt.setURIRoot(r.getRootResourceURI());
			p.setProperty(IRIArgument.BLANK_NODES.toString(), "false");
			p.setProperty(IRIArgument.ROOT.toString(), this.getRootResourceURI());
		}
		RemoteRepositoryManager manager = new RemoteRepositoryManager(this.getRepositoryURL());
		RemoteRepository rr = super.getContext().getBlazegraphClient().createAndGetRemoteRepositoryForNamespace(manager,
				this.getTargetNamespace(), this.getNamespaceProperties());

		Model m = ModelFactory.createDefaultModel();
		logger.trace("Reading file {}", this.getPayload().toString());
		RDFDataMgr.read(m, new StringReader(this.getPayload().toString()), "", Lang.JSONLD);
		logger.trace("Read " + m.size() + " triples from JSON-LD format!");
		if (m.size() == 0) {
			logger.trace("Trying to transform JSON document to RDF.");
//			m = jt.getModel(r.getPayload());
			m = ModelFactory
					.createModelForGraph(jt.triplify(p, new BaseFacadeXBuilder("uploader", p)).getDefaultGraph());
			logger.trace("Read " + m.size() + " triples from JSON!");
		}

		String rdfFile = RDFUploaderConfiguration.getInstance().getTmpFolder() + "/" + System.nanoTime() + ".nt";
		m.write(new FileOutputStream(new File(rdfFile)), "NT");
		if (this.getGraphURI() != null) {
			String nqFile = RDFUploaderConfiguration.getInstance().getTmpFolder() + "/" + System.nanoTime() + ".nq";
			RDFDataMgr.writeQuads(new FileOutputStream(new File(nqFile)),
					new IteratorQuadFromTripleIterator(
							RDFDataMgr.createIteratorTriples(new FileInputStream(new File(rdfFile)), Lang.NT, ""),
							this.getGraphURI()));
			rr.add(new RemoteRepository.AddOp(new File(nqFile), RDFFormat.NQUADS));
			new File(nqFile).delete();
		} else {
			rr.add(new RemoteRepository.AddOp(new FileInputStream(new File(rdfFile)), RDFFormat.NTRIPLES));
		}
		manager.close();
		m.close();
		new File(rdfFile).delete();
		logger.trace("Create Dataset Request - Accomplished");
	}

	@Override
	public String getDocId() {
		return null;
	}

}
