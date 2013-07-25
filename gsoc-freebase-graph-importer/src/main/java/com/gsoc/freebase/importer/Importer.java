package com.gsoc.freebase.importer;

import java.util.Set;

/**
 * <p>Interface to use by the importer implementations</p>
 * 
 * @author Antonio David Perez Morales <adperezmorales@gmail.com>
 */
public interface Importer
{
    /**
     * <p>Performs the import process</p>
     */
    public void process();
    
    /**
     * <p>Sets the importer step used when the information is being processed</p>
     * <p>Acts as a listener, because
     * @param step the {@code ImportListener} instance to add
     */
    public void addStep(ImporterStep step);
    
    /**
     * <p>Gets the steps configured for the importer</p>
     * @return the {@code Set<ImporterStep} instance
     */
    public Set<ImporterStep> getSteps();

}
