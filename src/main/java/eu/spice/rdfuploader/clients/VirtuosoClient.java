package eu.spice.rdfuploader.clients;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.sparql.util.FmtUtils;
import org.apache.jena.sparql.serializer.SerializationContext;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * Virtuoso backend client. All operations - RDF reads/writes and graph-group
 * management - go through a single JDBC channel (HikariCP pool). SPARQL is
 * issued as Virtuoso's "SPARQL ..." SQL extension.
 *
 * Namespaces map to Virtuoso graph groups. A document's named graph is added
 * as a member of its dataset's graph group on upload.
 */
public class VirtuosoClient implements TripleStoreClient {

    private static final Logger logger = LoggerFactory.getLogger(VirtuosoClient.class);

    private static final String DRIVER_CLASS = "virtuoso.jdbc4.Driver";
    private static final int MAX_POOL_SIZE = 4;
    // Number of triples per INSERT DATA batch. Tunable; keeps each SPARQL
    // statement well within Virtuoso's statement-size limits.
    private static final int INSERT_BATCH_SIZE = 1000;

    private final HikariDataSource dataSource;

    public VirtuosoClient(String jdbcUrl, String user, String password) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(user);
        config.setPassword(password);
        config.setDriverClassName(DRIVER_CLASS);
        config.setMaximumPoolSize(MAX_POOL_SIZE);
        config.setPoolName("virtuoso-pool");
        this.dataSource = new HikariDataSource(config);
    }

    // ---------------------------------------------------------------
    // Lifecycle
    // ---------------------------------------------------------------

    @Override
    public void testConnection() throws Exception {
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            s.execute("SELECT 1");
        }
    }

    @Override
    public void close() throws Exception {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
        }
    }

    // ---------------------------------------------------------------
    // Management plane (graph groups)
    // ---------------------------------------------------------------

    @Override
    public void createNamespace(String namespace) throws Exception {
        // quiet=1: do not error if the group already exists (idempotent)
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement("DB.DBA.RDF_GRAPH_GROUP_CREATE(?, 1)")) {
            ps.setString(1, namespace);
            ps.execute();
            logger.trace("Graph group {} created (or already existed)", namespace);
        }
    }

    @Override
    public boolean namespaceExists(String namespace) throws Exception {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c
                     .prepareStatement("SELECT 1 FROM DB.DBA.RDF_GRAPH_GROUP WHERE RGG_IRI = ?")) {
            ps.setString(1, namespace);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    @Override
    public boolean dropNamespace(String namespace) throws Exception {
        // Destructive parity with Blazegraph: clear every member graph's triples,
        // then drop the group. Done in one transaction so a mid-failure does not
        // leave a partially-cleared namespace.
        Connection c = null;
        try {
            c = dataSource.getConnection();
            c.setAutoCommit(false);

            List<String> members = listGroupMembers(c, namespace);
            if (members.isEmpty() && !namespaceExists(namespace)) {
                c.rollback();
                return false;
            }

            try (Statement s = c.createStatement()) {
                for (String memberGraph : members) {
                    s.execute("SPARQL CLEAR GRAPH <" + memberGraph + ">");
                    logger.trace("Cleared member graph {}", memberGraph);
                }
            }

            try (PreparedStatement ps = c.prepareStatement("DB.DBA.RDF_GRAPH_GROUP_DROP(?, 1)")) {
                ps.setString(1, namespace);
                ps.execute();
            }

            c.commit();
            logger.trace("Dropped graph group {} ({} members cleared)", namespace, members.size());
            return true;
        } catch (SQLException e) {
            rollbackQuietly(c);
            throw e;
        } finally {
            restoreAndClose(c);
        }
    }

    /**
     * Returns the member graph IRIs of a group by joining the group table to the
     * member table. Verify-against-instance: exact join/column semantics depend on
     * the Virtuoso build.
     */
    private List<String> listGroupMembers(Connection c, String namespace) throws SQLException {
        List<String> members = new ArrayList<>();
        String sql = "SELECT m.RGGM_MEMBER_IID FROM DB.DBA.RDF_GRAPH_GROUP g, DB.DBA.RDF_GRAPH_GROUP_MEMBER m "
                + "WHERE g.RGG_IID = m.RGGM_GROUP_IID AND g.RGG_IRI = ?";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, namespace);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    members.add(rs.getString(1));
                }
            }
        }
        return members;
    }

    // ---------------------------------------------------------------
    // Data plane
    // ---------------------------------------------------------------

    @Override
    public void uploadModel(Model m, String namespace, String graphURI, boolean clearGraph) throws Exception {
        // One transaction: (optional clear) + batched insert + group membership.
        Connection c = null;
        try {
            c = dataSource.getConnection();
            c.setAutoCommit(false);

            try (Statement s = c.createStatement()) {
                if (clearGraph) {
                    s.execute("SPARQL CLEAR GRAPH <" + graphURI + ">");
                }
                insertModelBatched(s, m, graphURI);
            }

            // Ensure the graph is a member of its dataset's group.
            try (PreparedStatement ps = c.prepareStatement("DB.DBA.RDF_GRAPH_GROUP_INS(?, ?)")) {
                ps.setString(1, namespace);
                ps.setString(2, graphURI);
                ps.execute();
            }

            c.commit();
            logger.trace("Uploaded {} triples to graph {} in group {}", m.size(), graphURI, namespace);
        } catch (SQLException e) {
            rollbackQuietly(c);
            throw e;
        } finally {
            restoreAndClose(c);
        }
    }

    /**
     * Serialises the model into batched "SPARQL INSERT DATA { GRAPH <g> { ... } }"
     * statements. Terms are formatted with Jena's FmtUtils to handle literal
     * escaping, language tags, datatypes, IRIs and blank nodes correctly.
     */
    private void insertModelBatched(Statement s, Model m, String graphURI) throws SQLException {
        SerializationContext sCxt = new SerializationContext();
        StringBuilder sb = new StringBuilder();
        int count = 0;

        ExtendedIterator<Triple> it = m.getGraph().find();
        try {
            while (it.hasNext()) {
                Triple t = it.next();
                sb.append(FmtUtils.stringForTriple(t, sCxt)).append(" .\n");
                count++;
                if (count % INSERT_BATCH_SIZE == 0) {
                    flushInsertBatch(s, graphURI, sb);
                    sb.setLength(0);
                }
            }
        } finally {
            it.close();
        }
        if (sb.length() > 0) {
            flushInsertBatch(s, graphURI, sb);
        }
    }

    private void flushInsertBatch(Statement s, String graphURI, StringBuilder triples) throws SQLException {
        String sparql = "SPARQL INSERT DATA { GRAPH <" + graphURI + "> {\n" + triples + "} }";
        s.execute(sparql);
    }

    @Override
    public Model executeConstructQuery(String query, String namespace) {
        // Confinement: the group IRI is set as the default graph so the query is
        // scoped to the group's members (Virtuoso expands a group named in FROM /
        // default-graph into its member graphs).
        // Verify-against-instance: both the confinement mechanism and the
        // reconstruction of the CONSTRUCT result below should be validated against
        // the live Virtuoso instance.
        Model result = ModelFactory.createDefaultModel();
        String wrapped = "SPARQL DEFINE input:default-graph-uri <" + namespace + "> " + stripSparqlPrefix(query);
        try (Connection c = dataSource.getConnection();
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery(wrapped)) {
            while (rs.next()) {
                // CONSTRUCT over Virtuoso JDBC returns S, P, O columns.
                // Reconstruction of typed/lang literals vs IRIs vs bnodes is the
                // fiddly part to verify on a real instance.
                String subj = rs.getString(1);
                String pred = rs.getString(2);
                String obj = rs.getString(3);
                result.add(result.createResource(subj), result.createProperty(pred), parseObject(result, obj));
            }
        } catch (SQLException e) {
            logger.error("Error executing construct query on group {}: {}", namespace, e.getMessage());
            throw new RuntimeException(e);
        }
        return result;
    }

    @Override
    public void clearGraph(String namespace, String graphURI) throws Exception {
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            s.execute("SPARQL CLEAR GRAPH <" + graphURI + ">");
            logger.trace("Cleared graph {}", graphURI);
        }
    }

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

    private org.apache.jena.rdf.model.RDFNode parseObject(Model model, String obj) {
        // Placeholder term reconstruction. VERIFY-AGAINST-INSTANCE: replace with
        // proper detection of IRI vs literal vs typed/lang literal vs bnode based
        // on what the Virtuoso JDBC driver actually returns for the O column.
        if (obj != null && (obj.startsWith("http://") || obj.startsWith("https://") || obj.startsWith("urn:"))) {
            return model.createResource(obj);
        }
        return model.createLiteral(obj);
    }

    private String stripSparqlPrefix(String query) {
        // The query coming from the job is plain SPARQL; Virtuoso's SQL wrapper
        // already prepends "SPARQL ...", so ensure we do not double it.
        String trimmed = query.trim();
        if (trimmed.regionMatches(true, 0, "SPARQL", 0, 6)) {
            return trimmed.substring(6).trim();
        }
        return trimmed;
    }

    private void rollbackQuietly(Connection c) {
        if (c != null) {
            try {
                c.rollback();
            } catch (SQLException ex) {
                logger.error("Rollback failed: {}", ex.getMessage());
            }
        }
    }

    private void restoreAndClose(Connection c) {
        if (c != null) {
            try {
                c.setAutoCommit(true);
            } catch (SQLException ex) {
                logger.error("Restoring autocommit failed: {}", ex.getMessage());
            }
            try {
                c.close();
            } catch (SQLException ex) {
                logger.error("Closing connection failed: {}", ex.getMessage());
            }
        }
    }
}