
package eu.spice.rdfuploader.clients;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.engine.main.QC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.sparqlanything.engine.FacadeX;

import eu.spice.rdfuploader.RDFUploaderConfiguration;

public class SPARQLAnythingClient {

	private static SPARQLAnythingClient instance;
	private Dataset kb;
	private String saQueryPattern;
	private static final Logger logger = LoggerFactory.getLogger(SPARQLAnythingClient.class);

	private SPARQLAnythingClient(RDFUploaderConfiguration conf) throws IOException {
		kb = DatasetFactory.createGeneral();
		QC.setFactory(ARQ.getContext(), FacadeX.ExecutorFactory);
		saQueryPattern = new String(Files.readAllBytes(new File(conf.getSPARQLAnythingFilepath()).toPath()));
	}

	public static SPARQLAnythingClient getInstance(RDFUploaderConfiguration conf) throws IOException {
		if (instance == null) {
			instance = new SPARQLAnythingClient(conf);
		}
		return instance;
	}

	public Model triplifyFile(String location, String root) {
		String queryString = String.format(saQueryPattern, location, root);
		Query query = QueryFactory.create(queryString);
		logger.trace("Executing query \n{}", query.toString(Syntax.defaultQuerySyntax));
		QueryExecution qExec = QueryExecutionFactory.create(query, kb);
		Model m = qExec.execConstruct();
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		m.write(baos, "TTL");
		logger.trace("Result \n{}", new String(baos.toByteArray()));
		return m;
	}

}
