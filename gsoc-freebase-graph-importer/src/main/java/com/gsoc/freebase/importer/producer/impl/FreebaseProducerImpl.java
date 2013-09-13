package com.gsoc.freebase.importer.producer.impl;

import java.io.File;
import java.io.FilenameFilter;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.jena.riot.RiotReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsoc.freebase.importer.ImporterConstants;
import com.gsoc.freebase.importer.model.Entity;
import com.gsoc.freebase.importer.producer.FreebaseProducer;

/**
 * <p>
 * Freebase Producer Implementation
 * </p>
 * <p>
 * This producer is responsible of read the entities from a directory or file and produce entities in a queue to be
 * consumed by the consumers
 * </p>
 * 
 * @author Antonio David Perez Morales <adperezmorales@gmail.com>
 * 
 */
public class FreebaseProducerImpl implements FreebaseProducer
{
    /**
     * Constant containing the default queue capacity
     */
    private static int DEFAULT_QUEUE_CAPACITY = 10;

    /**
     * Logger
     */
    private static Logger logger = LoggerFactory.getLogger(FreebaseProducerImpl.class);

    /**
     * Queue used to put entities in
     */
    private BlockingQueue<Entity> queue;

    /**
     * The file containing the file or directory to read entities from
     */
    private File file;

    /**
     * <p>
     * Default constructor
     * </p>
     * <p>
     * Initializes the queue with a capacity of 10
     * </p>
     */
    public FreebaseProducerImpl()
    {
        this.queue = new ArrayBlockingQueue<>(DEFAULT_QUEUE_CAPACITY);
        this.file = null;
    }

    /**
     * <p>
     * Creates a producer using the given queue to use and file to process
     * </p>
     * 
     * @param queue the {@code BlockingQueue<Entity>} to use
     * @param file the {@code File} file to be processed
     * 
     */
    public FreebaseProducerImpl(BlockingQueue<Entity> queue, File file)
    {
        this.queue = queue;
        this.file = file;
    }

    /**
     * <p>
     * Sets the queue to be used to put {@code Entity} instances on
     * </p>
     * 
     * @param queue
     */
    public void setQueue(BlockingQueue<Entity> queue)
    {
        this.queue = queue;
    }

    /**
     * <p>
     * Executes the process. Read triples from Freebase files and produce entities which will be consumed by consumers
     * listening the queue.
     * </p>
     */
    @Override
    public void run()
    {
        logger.info("Starting the producer " + Thread.currentThread().getName());
        /*
         * Process all files inside this directory
         */
        if (file.isDirectory())
        {
            logger.debug(file.getAbsolutePath() + " is a directory. Processing directory files (one level only)");

            File[] files = file.listFiles(new FilenameFilter()
            {
                @Override
                public boolean accept(File dir, String name)
                {
                    if (name.startsWith("."))
                        return false;
                    else
                        return true;
                }
            });

            for (File f : files)
            {
                this.processFile(f);
            }
        }
        /*
         * Process the file
         */
        else
        {
            this.processFile(this.file);
        }

        try
        {
            // Sleep a second to wait other threads to finish their processes
            Thread.sleep(1000);
        }
        catch (InterruptedException e)
        {

            e.printStackTrace();
        }

        logger.info("Finishing producer " + Thread.currentThread().getName());

        /*
         * Generates a dummy Entity to stop the consumers
         */
        Entity e = new Entity(ImporterConstants.CANCEL_ENTITY_URI, null);

        try
        {
            queue.put(e);
        }
        catch (InterruptedException e1)
        {
            e1.printStackTrace();
        }

    }

    /**
     * <p>
     * Process the file
     * </p>
     * <p>
     * In this case, a {@code RiotReader} object is used with the current step
     * </p>
     * 
     * @param f the file to process
     * @param step the current step. It is instance of {@code StreamRDF}
     */
    private void processFile(File f)
    {
        logger.debug(Thread.currentThread().getName() + " Processing file " + f.getAbsolutePath());
        long start = System.currentTimeMillis();
        FreebaseProducerImporterHandler handler = new FreebaseProducerImporterHandler(this.queue);
        RiotReader.parse(f.getAbsolutePath(), handler);
        long end = System.currentTimeMillis();
        logger.debug(Thread.currentThread().getName() + " File " + f.getAbsolutePath()+ " processed in "
                + (end - start) / 1000 + " seconds");
    }

}
