package com.gsoc.freebase.importer.model;

import java.util.List;
import java.util.Map;

/**
 * <p>
 * Entity class
 * </p>
 * <p>
 * Represents an entity in the model containing an URI identifying it and properties
 * </p>
 * 
 * @author Antonio David Perez Morales <adperezmorales@gmail.com>
 * 
 */
public class Entity
{
    /**
     * <p>
     * The URI of the entity
     * </p>
     */
    private String uri;

    /**
     * <p>
     * The properties of the entity
     * </p>
     */
    private Map<String, List<String>> properties;

    /**
     * <p>Constructor</p>
     * 
     * @param uri the URI of the entity
     * @param properties the properties of the entity
     */
    public Entity(String uri, Map<String, List<String>> properties)
    {
        this.uri = uri;
        this.properties = properties;
    }
    
    /**
     * <p>
     * Gets the URI
     * </p>
     * 
     * @return the URI of the entity
     */
    public String getUri()
    {
        return uri;
    }

    /**
     * <p>
     * Sets the URI of the entity
     * </p>
     * 
     * @param uri the URI of the entity
     */
    public void setUri(String uri)
    {
        this.uri = uri;
    }

    /**
     * <p>
     * Gets the properties of the entity
     * </p>
     * 
     * @return The properties of the entity 
     */
    public Map<String, List<String>> getProperties()
    {
        return properties;
    }

    /**
     * <p>Sets the properties of the entity</p>
     * @param properties The properties of the entity
     */
    public void setProperties(Map<String, List<String>> properties)
    {
        this.properties = properties;
    }

}
