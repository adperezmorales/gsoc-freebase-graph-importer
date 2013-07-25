package com.gsoc.freebase.importer;

/**
 * <p>Class containing Constants used by other classes</p>
 * @author Antonio David Perez Morales <adperezmorales@gmail.com>
 *
 */
public class ImporterConstants
{
    /**
     * Freebase namespace
     */
    public static final String FREEBASE_NAMESPACE = "http://rdf.freebase.com/ns/";
    
    /**
     * RDF namespace
     */
    public static final String RDF_NAMESPACE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
    
    /**
     * RDF Type property
     */
    public static final String RDF_TYPE = RDF_NAMESPACE+"type";
    
    /**
     * Freebase common topic property
     */
    public static final String FREEBASE_COMMON_TOPIC = FREEBASE_NAMESPACE+"common.topic";
    
    /**
     * Property in a Vertex to put the URI
     */
    public static final String VERTEX_ENTITY_URI_PROPERTY = "URI";
    
    /**
     * Edge label representing direct connections
     */
    public static final String DIRECT_CONNECTION_EDGE_LABEL = "direct-connection";
    
    /**
     * Edge label representing mediated connections
     */
    public static final String MEDIATED_CONNECTION_EDGE_LABEL = "mediated-connection";
    
}
