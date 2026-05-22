package eu.spice.rdfuploader.clients;

import org.apache.jena.rdf.model.Model;

/**
 * Backend-agnostic triple store client.
 *
 * Data-plane methods operate on named graphs scoped by a "namespace"
 * (Blazegraph namespace; Virtuoso graph group). Management-plane methods
 * manage the lifecycle of those namespaces.
 */
public interface TripleStoreClient {

    // --- Data plane ---

    Model executeConstructQuery(String query, String namespace);

    void uploadModel(Model m, String namespace, String graphURI, boolean clearGraph) throws Exception;

    void clearGraph(String namespace, String graphURI) throws Exception;

    // --- Management plane ---

    boolean namespaceExists(String namespace) throws Exception;

    boolean dropNamespace(String namespace) throws Exception;

    void createNamespace(String namespace) throws Exception;

    // --- Lifecycle ---

    void testConnection() throws Exception;

    void close() throws Exception;
}