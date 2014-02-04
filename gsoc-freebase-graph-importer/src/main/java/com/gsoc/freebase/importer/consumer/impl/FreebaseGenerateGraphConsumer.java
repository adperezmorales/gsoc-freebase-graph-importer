package com.gsoc.freebase.importer.consumer.impl;

import com.gsoc.freebase.importer.ImporterConstants;
import com.gsoc.freebase.importer.consumer.FreebaseConsumer;
import com.gsoc.freebase.importer.model.Entity;
import com.gsoc.freebase.importer.utils.FreebaseUtils;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

/**
 * <p>
 * Class representing a consumer
 * </p>
 * <p>
 * It is responsible of create the graph structure (Vertices)
 * </p>
 *
 * @author Antonio David Perez Morales <adperezmorales@gmail.com>
 */
public class FreebaseGenerateGraphConsumer implements FreebaseConsumer {
    /**
     * Logger
     */
    private static Logger logger = LoggerFactory.getLogger(FreebaseGenerateGraphConsumer.class);

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
     * @param queue      the {@code BlockingQueue<Entity>} used to consume entities
     * @param graph      the {@code Graph} instance used to store the vertices
     */
    public FreebaseGenerateGraphConsumer(CountDownLatch startLatch, BlockingQueue<Entity> queue, Graph graph) {
        this.startLatch = startLatch;
        this.queue = queue;
        this.graph = graph;

        /* Init the graph if needed */
        initGraph();
    }

    /**
     * <p>
     * Initialize the graph. Generates an index for the Vertex URI property in case that it doesn't exist yet and the current graph implementation supports it
     * </p>
     * <p>
     * This index is used later to looking for vertices in a faster way (managed by the graph implementation)
     * </p>
     */
    private void initGraph() {
        if (this.graph.getFeatures().supportsVertexKeyIndex) {
            KeyIndexableGraph keyIndexableGraph = (KeyIndexableGraph) this.graph;
            if (!keyIndexableGraph.getIndexedKeys(Vertex.class).contains(ImporterConstants.VERTEX_ENTITY_URI_PROPERTY)) {
                synchronized (this.graph) {
                    keyIndexableGraph.createKeyIndex(ImporterConstants.VERTEX_ENTITY_URI_PROPERTY, Vertex.class);
                    /* Commit the vertex index if necessary */
                    if (this.graph.getFeatures().supportsTransactions)

                    {
                        TransactionalGraph transactionalGraph = (TransactionalGraph) this.graph;
                        transactionalGraph.commit();
                    }
                }
            }
        }

    }

    @Override
    /**
     * <p>Run the consumer process</p>
     * <p>It generates a new Vertex for each consumed Entity</p>
     */
    public void run() {
        try {
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
            while (true) {
                Entity entity = queue.poll();
                if (entity == null)
                    continue;

                if (entity.getUri().equals(ImporterConstants.CANCEL_ENTITY_URI)) {
                    // Adding the entity to stop the process. Other consumers will consume this entity to stop their
                    // process
                    queue.put(entity);
                    break;
                }

                this.generateVertex(entity);

                /*
                 * Committing if the graph is transactional
                 */
                if (processed++ % 2000 == 0) {
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
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    /**
     * <p>
     * Generates a new vertex in the graph for the current entity
     * </p>
     *
     * @param entity the {@code Entity} object to be used to generate the vertex
     */
    private void generateVertex(Entity entity) {
        try {
            if (entity != null && entity.getProperties() != null) {
                // System.out.println("Processing entity: "+entity.getUri()+" in consumer "+Thread.currentThread().getName());
                if (FreebaseUtils.isTopic(entity.getProperties())) {
                    synchronized (this.graph) {
                        Vertex vertex = this.graph.addVertex(null);
                        vertex.setProperty(ImporterConstants.VERTEX_ENTITY_URI_PROPERTY, entity.getUri());

                        //blank value for image, will be overridden if it has one
                        vertex.setProperty("image", "novalue");
                    /*
                     * Adding only rdf-type and type.object.name properties
                     */
                        for (String property : entity.getProperties().keySet()) {
                            if (property.equals(ImporterConstants.RDF_TYPE)) {
                                vertex.setProperty(property, entity.getProperties().get(property));
                            }
                            if (property.equals(ImporterConstants.FREEBASE_TYPE_OBJECT_NAME)) {

                                vertex.setProperty("common_topic_name", entity.getProperties().get(property).get(0));
                                vertex.setProperty("common_topic_name_lc", entity.getProperties().get(property).get(0).toLowerCase());
                            }
                            if (property.contains("image")) {
                                //Map<String, List<String>> props = entity.getProperties();
                                vertex.setProperty("image", entity.getProperties().get(property).get(0));

                                //List<String> x = props.get(property);
                               // System.out.println("entity = " + entity.getUri());
                              //  for (String s : x) {
                               //     System.out.println("image property = " + s);

                                //}


                                //System.out.println("******************************" + property);
                            }
                            // System.out.println(property);
                        }

                        vertex = null;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
