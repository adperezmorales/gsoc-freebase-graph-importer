package com.gsoc.freebase.importer.producer.impl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import com.gsoc.freebase.importer.model.Entity;
import com.gsoc.freebase.importer.utils.FreebaseUtils;

/**
 * <p>
 * FreebaseProducerImporterHandler class
 * </p>
 * <p>
 * Used to produce entities to the shared queue
 * </p>
 * 
 * @author Antonio David Perez Morales <adperezmorales@gmail.com>
 * 
 */
public class FreebaseProducerImporterHandler extends AbstractProducerImporterHandler
{
    /**
     * Logger
     */
    private static Logger logger = Logger.getLogger(FreebaseProducerImporterHandler.class);
    
    /**
     * Queue used to put the entities
     */
    private BlockingQueue<Entity> queue;
    
    /**
     * <p>Constructs an instance of FreebaseProducerImporterHandler using the given queue to put the produced entities on</p>
     * @param queue the {@code BlockingQueue<Entity>} instance
     */
    public FreebaseProducerImporterHandler(BlockingQueue<Entity> queue) {
        this.queue = queue;
    }
    
    @Override
    public void onItemRead(String subject, Map<String, List<String>> properties)
    {
        if(FreebaseUtils.isFreebaseId(subject)) {
            Entity entity = new Entity(subject, properties);
            try
            {
                this.queue.put(entity);
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }
        
    }

}
