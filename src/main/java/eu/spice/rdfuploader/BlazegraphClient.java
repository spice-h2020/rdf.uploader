
package eu.spice.rdfuploader;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Properties;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.openrdf.model.Statement;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.rio.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdata.rdf.sail.webapp.SD;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;

import it.cnr.istc.stlab.lgu.commons.semanticweb.iterators.IteratorQuadFromTripleIterator;

public class BlazegraphClient {

	private String repositoryURL;

	public BlazegraphClient(String repositoryURL, String blazegraphNamespacePrefix) {
		super();
		this.repositoryURL = repositoryURL;
	}

	private static final Logger logger = LoggerFactory.getLogger(BlazegraphClient.class);

	public Model executeConstructQuery(String query, String namespace) {
		Query q = QueryFactory.create(query);
		String sparqlEndpointURL = repositoryURL + "/namespace/" + namespace + "/sparql";
		logger.trace("Executing {} on {}", q.toString(Syntax.defaultQuerySyntax), sparqlEndpointURL);
		QueryExecution qexec = QueryExecutionFactory.sparqlService(sparqlEndpointURL, q);
		return qexec.execConstruct();
	}

	public void uploadModel(Model m, String namespace, String graphURI, Properties namespaceProperties)
			throws Exception {
		uploadModel(m, namespace, graphURI, namespaceProperties, false);
	}

	public void uploadModel(Model m, String namespace, String graphURI, Properties namespaceProperties,
			boolean clearGraph) throws Exception {
		RemoteRepositoryManager manager = new RemoteRepositoryManager(this.getRepositoryURL());
		RemoteRepository rr = createAndGetRemoteRepositoryForNamespace(manager, namespace, namespaceProperties);
		String rdfFile = RDFUploaderConfiguration.getInstance().getTmpFolder() + "/" + System.nanoTime() + ".nt";
		m.write(new FileOutputStream(new File(rdfFile)), "NT");
		if (graphURI != null) {
			String nqFile = RDFUploaderConfiguration.getInstance().getTmpFolder() + "/" + System.nanoTime() + ".nq";
			RDFDataMgr.writeQuads(new FileOutputStream(new File(nqFile)), new IteratorQuadFromTripleIterator(
					RDFDataMgr.createIteratorTriples(new FileInputStream(new File(rdfFile)), Lang.NT, ""), graphURI));

			if (clearGraph) {
				rr.prepareUpdate("CLEAR GRAPH <" + graphURI + ">").evaluate();
			}

			rr.add(new RemoteRepository.AddOp(new File(nqFile), RDFFormat.NQUADS));
			new File(nqFile).delete();
		} else {
			rr.add(new RemoteRepository.AddOp(new FileInputStream(new File(rdfFile)), RDFFormat.NTRIPLES));
		}
		manager.close();
		new File(rdfFile).delete();
	}

	public String getRepositoryURL() {
		return repositoryURL;
	}

	public boolean namespaceExists(String namespace) throws Exception {
		RemoteRepositoryManager repo = new RemoteRepositoryManager(this.getRepositoryURL());
		boolean result = namespaceExists(repo, namespace);
		repo.close();
		return result;
	}

	public boolean dropNamespace(String namespace) throws Exception {
		RemoteRepositoryManager repo = new RemoteRepositoryManager(this.getRepositoryURL());
		boolean result = namespaceExists(repo, namespace);
		if (result) {
			repo.deleteRepository(namespace);
		}
		repo.close();
		return result;
	}

	public boolean namespaceExists(RemoteRepositoryManager repo, String namespace) throws Exception {
		GraphQueryResult res = repo.getRepositoryDescriptions();
		try {
			while (res.hasNext()) {
				Statement stmt = res.next();
				if (stmt.getPredicate().toString().equals(SD.KB_NAMESPACE.stringValue())) {
					if (namespace.equals(stmt.getObject().stringValue())) {
						return true;
					}
				}
			}
		} finally {
			res.close();
		}
		return false;
	}

	public RemoteRepository createAndGetRemoteRepositoryForNamespace(RemoteRepositoryManager manager, String namespace,
			Properties namespaceProperties) throws Exception {
		logger.trace("Create " + namespace + " namepsace.");
		if (!namespaceExists(manager, namespace)) {
			manager.createRepository(namespace, namespaceProperties);
			logger.trace("Namespace " + namespace + " created!");
		} else {
			logger.trace("Namespace " + namespace + " already exists!");
		}

		logger.trace("Namespace " + namespace + " created!");
		return manager.getRepositoryForNamespace(namespace);
	}

	public void clearGraph(String namespace, Properties properties, String graphURI) throws Exception {
		RemoteRepositoryManager manager = new RemoteRepositoryManager(this.getRepositoryURL());
		RemoteRepository rr = createAndGetRemoteRepositoryForNamespace(manager, namespace, properties);
		rr.prepareUpdate("CLEAR GRAPH <" + graphURI + ">").evaluate();
		logger.trace("CLEAR graph " + graphURI + " completed!");
		manager.close();
	}

//	public void dropAllNamespaces() throws Exception {
//		RemoteRepositoryManager repo = new RemoteRepositoryManager(this.getRepositoryURL());
//		GraphQueryResult gqr = repo.getRepositoryDescriptions();
//		Map<String, String> namespaces = gqr.getNamespaces();
//		namespaces.forEach((k, v) -> System.out.println(k + " " + v));
//	}

}
