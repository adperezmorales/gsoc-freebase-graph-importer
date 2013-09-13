package com.gsoc.freebase.importer.consumer.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsoc.freebase.importer.ImporterConstants;
import com.gsoc.freebase.importer.consumer.FreebaseConsumer;
import com.gsoc.freebase.importer.model.Entity;
import com.gsoc.freebase.importer.utils.FreebaseUtils;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;

/**
 * <p>
 * Class representing a consumer
 * </p>
 * <p>
 * It is responsible of create the graph relationships (Edges) using the entities being consumed and the entities
 * existing in the graph
 * </p>
 * 
 * @author Antonio David Perez Morales <adperezmorales@gmail.com>
 * 
 */
public class FreebaseGenerateRelationsConsumer implements FreebaseConsumer
{

    /**
     * Logger
     */
    private static Logger logger = LoggerFactory.getLogger(FreebaseGenerateRelationsConsumer.class);

    /**
     * The graph used
     */
    private Graph graph;

    /**
     * Latch used to wait before start
     */
    private CountDownLatch startLatch;

    /**
     * Queue used to get and consume entities
     */
    private BlockingQueue<Entity> queue;

    /**
     * <p>
     * Constructs an instance of FreebaseGenerateGraphConsumer using the given latch, queue and graph
     * </p>
     * 
     * @param startLatch the {@code CountDownLatch} object used to wait before start the process
     * @param queue the {@code BlockingQueue<Entity>} used to consume entities
     * @param graph the {@code Graph} instance used to store the vertices
     */
    public FreebaseGenerateRelationsConsumer(CountDownLatch startLatch, BlockingQueue<Entity> queue, Graph graph)
    {
        this.startLatch = startLatch;
        this.queue = queue;
        this.graph = graph;

        /* Init the graph if needed */
        initGraph();
    }

    /**
     * <p>
     * Initialize the graph. Generates an index for the Edges using a property containing the URI of the connected
     * entities, in case it doesn't exist yet and the current graph implementation supports it
     * </p>
     * <p>
     * This index is used later to looking for edges in a faster way (managed by the graph implementation)
     * </p>
     */
    private void initGraph()
    {
        if (this.graph.getFeatures().supportsEdgeKeyIndex)
        {
            KeyIndexableGraph keyIndexableGraph = (KeyIndexableGraph) this.graph;
            if (!keyIndexableGraph.getIndexedKeys(Edge.class).contains(
                    ImporterConstants.EDGE_KEY_VERTICES_CONNECTED_PROPERTY))
            {
                keyIndexableGraph.createKeyIndex(ImporterConstants.EDGE_KEY_VERTICES_CONNECTED_PROPERTY, Edge.class);
                /* Commit the edge index if necessary */
                if (this.graph.getFeatures().supportsTransactions)

                {
                    TransactionalGraph transactionalGraph = (TransactionalGraph) this.graph;
                    transactionalGraph.commit();
                }

            }

        }

    }

    @Override
    /**
     * <p>Run the consumer process</p>
     * <p>It generates a new Edge for each connected entities (Vertices in the graph)</p>
     */
    public void run()
    {
        try
        {
            logger.debug("Waiting the start in " + Thread.currentThread().getName());
            /*
             * Wait in the latch until other process tells this process to start
             */
            startLatch.await();

            logger.debug("Starting the consumer " + Thread.currentThread().getName());

            int processed = 1;

            /*
             * Infinite loop to consume entities when produced until a certain Entity comes.
             */
            while (true)
            {
                Entity entity = queue.take();

                if (entity == null)
                    continue;

                if (entity.getUri().equals(ImporterConstants.CANCEL_ENTITY_URI))
                {
                    // Adding the entity to stop the process. Other consumers will consume this entity to stop their
                    // process
                    queue.put(entity);
                    logger.info("Cancel consumer");
                    break;
                }
                
                //logger.debug("Processing entity: "+entity.getUri());

                this.generateRelations(entity);
                if (processed++ % 300 == 0)
                {

                    if (this.graph.getFeatures().supportsTransactions)

                    {
                        TransactionalGraph transactionalGraph = (TransactionalGraph) this.graph;
                        transactionalGraph.commit();
                    }

                }

            }

            /*
             * Commiting the remaining transactions if the graph is transactional
             */
            if (this.graph.getFeatures().supportsTransactions)

            {
                TransactionalGraph transactionalGraph = (TransactionalGraph) this.graph;
                transactionalGraph.commit();
            }

            logger.debug("Finishing " + Thread.currentThread().getName());
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    /**
     * <p>
     * Create the graph relations using the next algorithm:
     * <ul>
     * <li>If the entity is a topic, search the properties to see if there are properties which references (single
     * value) to other topics and create a direct relation between them (same 'multivalue' property referencing several
     * entities is ignored)</li>
     * <li>If the entity is not a topic, search the properties to see if there are properties which references (single
     * value) to other topics and create a mediated relation.</li>
     * <ul>
     * <p>
     * A direct relation creates an edge between topic vertex and the predicate referencing them is set as edge property
     * using the next rule: Property: music.recording.canonical_version Edge properties: music = music + 1
     * music.recording = music.recording + 1 music.recording.canonical_version = music.recording.canonical_version + 1;
     * </p>
     * <p>
     * A mediated relation creates edges between the topics which a non topic entity is referencing
     * </p>
     * <p>
     * Topic 1 <---- predicate --- {NotTopicMediator} --- predicate2 ----> Topic 2
     * </p>
     * <p>
     * For example: - [NotTopic predicate.ns.v Topic1] - [NotTopic other_predicate.ons.z Topic2] - [NotTopic pr.edi.cate
     * Topic3]
     * 
     * This example creates the following edges: - Topic1 <-----> Topic2 with properties: predicate.ns ,
     * other_predicate.ons.z (using the same rules as direct relations) - Topic1 <-----> Topic3 with properties:
     * predicate.ns , pr.edi.cate - Topic2 <-----> Topic3 with properties: other_predicate.ons.z , pr.edi.cate
     * </p>
     */
    public void generateRelations(Entity entity)
    {
        if (FreebaseUtils.isTopic(entity.getProperties()))
        {
            synchronized (this.graph)
            {

                this.processTopic(entity);
            }
        }

        else
        {
            synchronized (this.graph)
            {
                this.processNotTopic(entity);
            }
        }
    }

    /**
     * <p>
     * Process a topic entity
     * </p>
     * 
     * @param subject the entity subject
     * @param properties the entity properties
     */
    private void processTopic(Entity entity)
    {
        for (String property : entity.getProperties().keySet())
        {
            List<String> list = entity.getProperties().get(property);
            if (list.size() > 1)
            {
                // Ignore multivalued properties
                continue;
            }

            /*
             * Using the first value (the only value because we are ignoring multivalued properties)
             */
            String value = list.get(0);

            if (FreebaseUtils.isFreebaseId(value))

                if (this.isTopicGraph(value))
                {
                    // Create direct relation
                    this.createDirectRelation(property, entity.getUri(), value);
                }

        }

    }

    /**
     * <p>
     * Process a not topic entity
     * </p>
     * 
     * @param subject the entity subject
     * @param properties the entity properties
     */
    private void processNotTopic(Entity entity)
    {
        // Create mediated relations
        List<Holder> prevRelatedSubjects = new ArrayList<Holder>();
        for (String property : entity.getProperties().keySet())
        {

            List<String> list = entity.getProperties().get(property);
            if (list.size() > 1)
            {
                // Ignore multivalued properties
                continue;
            }

            /*
             * Using the first value (the only value because we are ignoring multivalued properties)
             */
            String value = list.get(0);

            if (FreebaseUtils.isFreebaseId(value))
            {
                if (this.isTopicGraph(value))
                {
                    Holder holder = new Holder();
                    holder.property = property;
                    holder.uri = value;
                    if (prevRelatedSubjects.size() > 0)
                    {
                        this.createMediatedDirectRelation(entity.getUri(), prevRelatedSubjects, holder);
                    }

                    prevRelatedSubjects.add(holder);
                }

            }
        }
    }

    /**
     * <p>
     * Check if the subject is a topic
     * </p>
     * <p>
     * That is to say, checks if exists a vertex in the graph with the supplied uri property
     * </p>
     * 
     * @param subject the subject to check
     * @return a boolean indicating if the subject is a topic or not
     */
    private boolean isTopicGraph(String subject)
    {
        Iterator<Vertex> it = graph.getVertices(ImporterConstants.VERTEX_ENTITY_URI_PROPERTY, subject).iterator();
        return it.hasNext();

    }

    /**
     * <p>
     * Create mediated relations between topics
     * </p>
     * 
     * @param noTopicSubject the entity subject (not topic) acting as a mediator of other entities
     * @param prevRelatedSubjects the list of previous entities which will be related to the current topic entity
     * @param holder the current entity to be related with the previous ones
     */
    private void createMediatedDirectRelation(String noTopicSubject, List<Holder> prevRelatedSubjects, Holder holder)
    {
        Vertex n = graph.getVertices(ImporterConstants.VERTEX_ENTITY_URI_PROPERTY, holder.uri).iterator().next();

        for (Holder prevHolder : prevRelatedSubjects)
        {
            Vertex v = graph.getVertices(ImporterConstants.VERTEX_ENTITY_URI_PROPERTY, prevHolder.uri).iterator().next();

            Edge e = this.getEdgeBetweenVertices(v, n, ImporterConstants.MEDIATED_CONNECTION_EDGE_LABEL);

            if (e == null)
            {
                e = graph.addEdge(null, n, v, ImporterConstants.MEDIATED_CONNECTION_EDGE_LABEL);
                e.setProperty(
                        ImporterConstants.EDGE_KEY_VERTICES_CONNECTED_PROPERTY,
                        n.getProperty((String) ImporterConstants.VERTEX_ENTITY_URI_PROPERTY) + "|"
                                + v.getProperty(ImporterConstants.VERTEX_ENTITY_URI_PROPERTY));
            }

            e.setProperty("connected-by", noTopicSubject);

            this.updateEdgeValues(e, holder.property);
            this.updateEdgeValues(e, prevHolder.property);

        }
    }

    /**
     * <p>
     * Creating direct relation between topics
     * </p>
     * 
     * @param property the property linking the subjects
     * @param inSubject one subject to be related
     * @param outSubject the other subject to be related
     */
    private void createDirectRelation(String property, String inSubject, String outSubject)
    {

        Vertex in = graph.getVertices(ImporterConstants.VERTEX_ENTITY_URI_PROPERTY, inSubject).iterator().next();
        Vertex out = graph.getVertices(ImporterConstants.VERTEX_ENTITY_URI_PROPERTY, outSubject).iterator().next();

        /*
         * If any of the vertices don't exist
         */
        if (in == null || out == null)
            return;

        Edge e = this.getEdgeBetweenVertices(in, out, ImporterConstants.DIRECT_CONNECTION_EDGE_LABEL);

        if (e == null)
        {

            e = graph.addEdge(null, in, out, ImporterConstants.DIRECT_CONNECTION_EDGE_LABEL);
            e.setProperty(
                    ImporterConstants.EDGE_KEY_VERTICES_CONNECTED_PROPERTY,
                    in.getProperty((String) ImporterConstants.VERTEX_ENTITY_URI_PROPERTY) + "|"
                            + out.getProperty(ImporterConstants.VERTEX_ENTITY_URI_PROPERTY));

        }

        this.updateEdgeValues(e, property);

    }

    /**
     * <p>
     * Get an existing edge between two vertices
     * </p>
     * 
     * @param in one vertex
     * @param out other vertex
     * @param label the edge label
     * @return the {@code Edge} connecting the two vertex or null if the edge doesn't exist
     */
    private Edge getEdgeBetweenVertices(Vertex in, Vertex out, String label)
    {
        /* Search in one sense in->out */
        Iterable<Edge> edges = this.graph.getEdges(ImporterConstants.EDGE_KEY_VERTICES_CONNECTED_PROPERTY,
                this.generateEdgeIndexKeyPropertyValue(in, out));
        for (Edge edge : edges)
        {
            if (edge.getLabel().equals(label))
                return edge;
        }

        /* Search in the other sense out->in */
        edges = this.graph.getEdges(ImporterConstants.EDGE_KEY_VERTICES_CONNECTED_PROPERTY,
                this.generateEdgeIndexKeyPropertyValue(in, out));
        for (Edge edge : edges)
        {
            if (edge.getLabel().equals(label))
                return edge;
        }
        return null;
    }

    /**
     * <p>
     * Generates the value of the index key property for the edges between two vertices
     * </p>
     * <p>
     * It returns a {@code String} containing the URI of each vertex joined by the '|' character
     * </p>
     * 
     * @param in a {@code Vertex} instance
     * @param out another {@code Vertex} instance
     * @return a {@code String} containing the generated value
     */
    private String generateEdgeIndexKeyPropertyValue(Vertex in, Vertex out)
    {
        return (String) in.getProperty(ImporterConstants.VERTEX_ENTITY_URI_PROPERTY) + "|"
                + out.getProperty(ImporterConstants.VERTEX_ENTITY_URI_PROPERTY);
    }

    /**
     * <p>
     * Update the values of a property in an edge
     * </p>
     * <p>
     * If the property is of the form of a.b.c then a, a.b and a.b.c will be updated. That is to say, to add one to the
     * value of that property in the edge or creating a new property in the edge with value of 1
     * </p>
     * 
     * @param e the edge
     * @param fullProperty the property to be updated
     */
    private void updateEdgeValues(Edge e, String fullProperty)
    {
        String prop = fullProperty.substring(fullProperty.lastIndexOf("/") + 1);
        String[] splitProp = prop.split("\\.");
        String last = "";
        for (String s : splitProp)
        {
            this.updateEdgeValue(e, last + s);
            last += s + ".";
        }
    }

    /**
     * <p>
     * Update the value of a property in the edge, adding one to the value if the property exists in the edge or
     * creating a new one with value of 1
     * </p>
     * 
     * @param e the edge
     * @param s the property to update
     */
    private void updateEdgeValue(Edge e, String s)
    {
        Object o = e.getProperty(s);
        int i;
        if (o == null)
            i = 1;
        else
        {
            i = (int) o;
            i++;
        }

        e.setProperty(s, i);
    }

    /**
     * <p>
     * Class to represents a Holder, containing information about a subject and a property
     * </p>
     * <p>
     * It is used in mediated-connections when building all possible relations between topic entities connected by a not
     * topic entity
     * 
     * @author Antonio David Perez Morales <adperezmorales@gmail.com>
     * 
     */
    private static class Holder
    {
        public String property;
        public String uri;
    }

}
