package com.gsoc.freebase.importer.impl;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsoc.freebase.importer.consumer.FreebaseConsumer;
import com.gsoc.freebase.importer.consumer.impl.FreebaseGenerateGraphConsumer;
import com.gsoc.freebase.importer.consumer.impl.FreebaseGenerateRelationsConsumer;
import com.gsoc.freebase.importer.model.Entity;
import com.gsoc.freebase.importer.producer.FreebaseProducer;
import com.gsoc.freebase.importer.producer.impl.FreebaseProducerImpl;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;

/**
 * <p>
 * Freebase importer to a Graph using Riot parser in the Producer
 * </p>
 * <p>
 * It parses the BaseKBLime Freebase data dump (in Turtle, RDF, NTriples format...) and store it in a graph
 * </p>
 * <p>
 * It uses files or directories to import into a Graph Database managed by Tinkerpop Blueprints
 * </p>
 * <p>
 * This importer creates a graph in several steps. This will process every file or directory for each step.
 * </p>
 * 
 * <p>
 * For example:
 * <ul>
 * <li>Create graph step: It will process every file or files (in the directory) to create the graph vertices
 * <li>Create graph relations step: It will process every file or files (in the directory) to create the graph
 * relations.
 * </ul>
 * </p>
 * <p>
 * In the previous example, the files are processed two times, one time for creating the graph structure and other time
 * for creating the relations and properties (kind of weights) in the edges
 * </p>
 * 
 * @author Antonio David Perez Morales <adperezmorales@gmail.com>
 * 
 */
public class FreebaseToGraphImporter
{
    /**
     * Logger
     */
    private static Logger logger = LoggerFactory.getLogger(FreebaseToGraphImporter.class);

    /**
     * Constants containing the default consumers size
     */
    private static int DEFAULT_CONSUMERS_SIZE = Runtime.getRuntime().availableProcessors();

    /**
     * Number of consumers for each step
     */
    private int consumerSize;

    /**
     * The file or directory to import
     */
    private File file;

    /**
     * Location of the graph
     */
    private File graphLocation;

    /**
     * The {@code Graph} implementation
     */
    private Graph graph;

    /**
     * Flag indicating whether to generate the graph or not
     */
    private boolean generateGraph;

    /**
     * Flag indicating whether to generate the graph relations
     */
    private boolean generateGraphRelations;

    /**
     * <p>
     * Constructor
     * </p>
     * <p>
     * Creates an importer to process the file or directory passed as parameter
     * </p>
     * 
     * @param file the string or directory to import
     */
    public FreebaseToGraphImporter(String file, String graphLocation)
    {
        this(new File(file), new File(graphLocation), DEFAULT_CONSUMERS_SIZE);
    }

    /**
     * <p>
     * Constructor
     * </p>
     * <p>
     * Creates an importer to process the file or directory passed as parameter
     * </p>
     * <p>
     * Initializes the set of steps
     * </p>
     * 
     * @param file the string or directory to import
     */
    public FreebaseToGraphImporter(File file, File graphLocation, int consumerSize)
    {
        this.file = file;
        this.graphLocation = graphLocation;
        this.consumerSize = consumerSize;
        this.generateGraph = false;
        this.generateGraphRelations = false;

    }

    /**
     * <p>
     * Set the generate graph flag
     * </p>
     * 
     * @param flag the value of the flag
     */
    public void setGenerateGraph(Boolean flag)
    {
        this.generateGraph = flag;
    }

    /**
     * <p>
     * Set the generate graph relations flag
     * </p>
     * 
     * @param flag the value of the flag
     */
    public void setGenerateGraphRelations(Boolean flag)
    {
        this.generateGraphRelations = flag;
    }

    /**
     * <p>
     * Build an instance using the given file name and the steps
     * </p>
     */
    public FreebaseToGraphImporter(String file, String graphLocation, int consumerSize)
    {
        this(new File(file), new File(graphLocation), consumerSize);
    }

    /**
     * <p>
     * Initialize the graph using the graph location
     * </p>
     * <p>
     * Uses Neo4jGraph implementation of Tinkerpop Blueprints
     * </p>
     */
    private void initializeGraph()
    {
        this.graph = new Neo4jGraph(this.graphLocation.getAbsolutePath());
    }

    /**
     * <p>
     * Performs the import process based using two types of consumers: FreebaseGenerateGraphConsumer and
     * FreebaseGenerateRelationsConsumer
     * </p>
     */
    public void process()
    {
        /* Run the parser process */
        try
        {
            logger.info("Freebase importer starts");
            long start = System.currentTimeMillis();
            /* Run the parser process */
            if (this.generateGraph)
            {
                this.generateGraph();
            }
            Thread.sleep(2000);
            if (this.generateGraphRelations)
            {
                this.generateGraphRelations();
            }
            long end = System.currentTimeMillis();
            logger.info("Freebase importer finished. Duration: " + (end - start) / 1000 + " seconds");
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }

    }

    /**
     * <p>
     * First step: Generate the graph
     * </p>
     */
    private void generateGraph()
    {
        /* Run the parser process */
        logger.info("Starting generate graph process");
        long start = System.currentTimeMillis();

        CountDownLatch startLatch = new CountDownLatch(1);
        BlockingQueue<Entity> queue = new ArrayBlockingQueue<>(this.consumerSize);

        this.initializeGraph();

        List<Thread> consumers = new ArrayList<>();
        for (int i = 0; i < this.consumerSize; i++)
        {

            FreebaseConsumer consumer = new FreebaseGenerateGraphConsumer(startLatch, queue, this.graph);

            Thread consumerThread = new Thread(consumer, FreebaseGenerateGraphConsumer.class.getName() + i);
            consumers.add(consumerThread);
            consumerThread.start();
        }

        FreebaseProducer producer = new FreebaseProducerImpl(queue, this.file);
        Thread producerThread = new Thread(producer, FreebaseProducerImpl.class.getName());
        try
        {
            // Waiting for initializarion of consumers
            Thread.sleep(500);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }

        // Start the consumers
        startLatch.countDown();

        // Start the producer
        producerThread.start();

        // waiting producer and consumers thread to finish
        try
        {
            producerThread.join();
            for (Thread t : consumers)
            {
                t.join();
            }
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }

        logger.debug("Committing pending transactions");
        this.graph.shutdown();

        long end = System.currentTimeMillis();

        logger.info("Generate graph (vertices) process finished. Duration: " + ((end - start) / 1000) + " seconds");

    }

    /**
     * <p>
     * Second step: Generate the graph relations
     * </p>
     */
    private void generateGraphRelations()
    {
        /* Run the parser process */
        logger.info("Starting generate graph relations process");
        long start = System.currentTimeMillis();

        CountDownLatch startLatch = new CountDownLatch(1);
        BlockingQueue<Entity> queue = new ArrayBlockingQueue<>(10);

        initializeGraph();

        /*
         * Creates and starts the create or update edges consumer Consume edge orders produced by
         * FreebaseGenerateRelationsConsumer
         */

        FreebaseConsumer consumer = new FreebaseGenerateRelationsConsumer(startLatch, queue, graph);

        Thread relationsThread = new Thread(consumer, FreebaseGenerateRelationsConsumer.class.getName());
        relationsThread.start();

        FreebaseProducer producer = new FreebaseProducerImpl(queue, this.file);
        Thread producerThread = new Thread(producer, FreebaseProducerImpl.class.getName());
        try
        {
            // Waiting for initializarion of consumers
            Thread.sleep(500);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }

        // Start the consumers
        startLatch.countDown();

        // Start the producer
        producerThread.start();

        // waiting producer and consumers thread to finish
        try
        {
            producerThread.join();
            relationsThread.join();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }

        logger.debug("Committing pending transactions");
        this.graph.shutdown();

        long end = System.currentTimeMillis();

        logger.info("Generate graph relations (edges) process finished. Duration: " + ((end - start) / 1000)
                + " seconds");
    }

}
