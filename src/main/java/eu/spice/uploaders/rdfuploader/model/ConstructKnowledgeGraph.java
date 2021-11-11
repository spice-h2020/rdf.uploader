package eu.spice.uploaders.rdfuploader.model;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Properties;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.openrdf.rio.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;

import eu.spice.rdfuploader.RDFUploaderConfiguration;
import eu.spice.rdfuploader.uploaders.Utils;
import it.cnr.istc.stlab.lgu.commons.semanticweb.iterators.IteratorQuadFromTripleIterator;

public class ConstructKnowledgeGraph implements Request {

	private String constructQuery;
	private String sourceNamespace;
	private String targetNamespace;
	private String repositoryURL;
	private String targetGraphURI;
	private Properties blazegraphProperties;
	private static final Logger logger = LoggerFactory.getLogger(ConstructKnowledgeGraph.class);

	public ConstructKnowledgeGraph(String constructQuery, String sourceNamespace, String targetNamespace,
			String repositoryURL, Properties blazegraphProperties) {
		super();
		this.constructQuery = constructQuery;
		this.sourceNamespace = sourceNamespace;
		this.targetNamespace = targetNamespace;
		this.repositoryURL = repositoryURL;
		this.blazegraphProperties = blazegraphProperties;
	}

	public String getConstructQuery() {
		return constructQuery;
	}

	public void setConstructQuery(String constructQuery) {
		this.constructQuery = constructQuery;
	}

	public String getSourceNamespace() {
		return sourceNamespace;
	}

	public void setSourceNamespace(String sourceNamespace) {
		this.sourceNamespace = sourceNamespace;
	}

	public String getTargetNamespace() {
		return targetNamespace;
	}

	public void setTargetNamespace(String targetNamespace) {
		this.targetNamespace = targetNamespace;
	}

	public String getRepositoryURL() {
		return repositoryURL;
	}

	public void setRepositoryURL(String repositoryURL) {
		this.repositoryURL = repositoryURL;
	}
	
	

	public String getTargetGraphURI() {
		return targetGraphURI;
	}

	public void setTargetGraphURI(String targetGraphURI) {
		this.targetGraphURI = targetGraphURI;
	}

	@Override
	public void accomplishRequest() throws Exception {

		Query q = QueryFactory.create(constructQuery);
		if (!q.isConstructType()) {
			logger.error("A CONSTRUCT query is needed for building a new Knowledge graph.");
			return;
		}

		logger.trace("SPARQL endpoint url {}", repositoryURL + "/namespace/" + sourceNamespace + "/sparql");

		QueryExecution qexec = QueryExecutionFactory
				.sparqlService(repositoryURL + "/namespace/" + sourceNamespace + "/sparql", q);

		RemoteRepositoryManager manager = new RemoteRepositoryManager(this.getRepositoryURL());
		RemoteRepository rr = Utils.createAndGetRemoteRepositoryForNamespace(manager, this.getTargetNamespace(),
				this.blazegraphProperties);

		if (this.targetGraphURI == null) {
			String ntFile = RDFUploaderConfiguration.getInstance().getTmpFolder() + "/" + System.nanoTime() + ".nt";
			FileOutputStream fos = new FileOutputStream(new File(ntFile));
			RDFDataMgr.writeTriples(fos, qexec.execConstructTriples());
			fos.flush();
			fos.close();
			rr.add(new RemoteRepository.AddOp(new File(ntFile), RDFFormat.NTRIPLES));
			new File(ntFile).delete();
		} else {
			logger.trace("Upload into graph {}", this.targetGraphURI);
			String nqFile = RDFUploaderConfiguration.getInstance().getTmpFolder() + "/" + System.nanoTime() + ".nq";
			FileOutputStream fos = new FileOutputStream(new File(nqFile));
			RDFDataMgr.writeQuads(fos,
					new IteratorQuadFromTripleIterator(qexec.execConstructTriples(), this.targetGraphURI));
			fos.flush();
			fos.close();
			rr.add(new RemoteRepository.AddOp(new File(nqFile), RDFFormat.NQUADS));
			new File(nqFile).delete();

		}
//		else if (q.isConstructQuad()) {
//			String ntFile = RDFUploaderConfiguration.getInstance().getTmpFolder() + "/" + System.nanoTime() + ".nq";
//			FileOutputStream fos = new FileOutputStream(new File(ntFile));
//			RDFDataMgr.writeQuads(fos, qexec.execConstructQuads());
//			fos.flush();
//			fos.close();
//			rr.add(new RemoteRepository.AddOp(new File(ntFile), RDFFormat.NQUADS));
//		}

		logger.trace("Knowledge Graph constructed");

	}

}
