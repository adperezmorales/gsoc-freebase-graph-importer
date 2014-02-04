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
    public static final String FREEBASE_NAMESPACE = "http://rdf.basekb.com/ns/";
    
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
    
    /**
     * Edge property containing the property used for generate an index for the edges
     */
    public static final String EDGE_KEY_VERTICES_CONNECTED_PROPERTY = "vertices.connected";
    
    /**
     * Freebase type object name
     */
    public static final String FREEBASE_TYPE_OBJECT_NAME = FREEBASE_NAMESPACE + "type.object.name";
    
    /**
     * Constant containing the URI of the entity to be used to cancel the consumers
     */
    public static final String CANCEL_ENTITY_URI = "CANCEL";
    
}
