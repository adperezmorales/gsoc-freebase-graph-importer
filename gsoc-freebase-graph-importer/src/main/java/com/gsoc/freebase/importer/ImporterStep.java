package com.gsoc.freebase.importer;

import java.util.List;
import java.util.Map;

/**
 * <p>Interface used to represent a step in the importer process</p>
 * @author Antonio David Perez Morales <adperezmorales@gmail.com>
 *
 */
public interface ImporterStep
{
    /**
     * <p>Called when an item has been read</p>
     * @param subject the item subject
     * @param properties containing the subject properties
     */
    public void onItemRead(String subject, Map<String, List<String>> properties);
    
    /**
     * <p>Called when the import process starts</p>
     */
    public void onImportStart();
    
    /**
     * <p>Called when the import process ends</p>
     */
    public void onImportEnd();
    
    /**
     * <p>Gets the order number for this step<p>
     * @return the number which this step will be processed by the importer
     */
    public int getOrder();
    
    /**
     * <p>Sets the the order number in which this step will be processed</p>
     * @param order the order number
     */
    public void setOrder(int order);

}