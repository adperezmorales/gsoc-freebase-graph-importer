package com.gsoc.freebase.importer.utils;

import java.util.List;
import java.util.Map;

import com.gsoc.freebase.importer.ImporterConstants;

/**
 * <p>Utility class for Freebase information</p>
 * @author Antonio David Perez Morales <adperezmorales@gmail.com>
 *
 */
public class FreebaseUtils
{
    /**
     * <p>Checks if the given URI is a Freebase ID
     * @param uri the URI to check
     * @return a boolean indicating whether the URI is a freebase Id or not
     */
    public static boolean isFreebaseId(String uri)
    {
        return uri.startsWith("http://rdf.basekb.com/ns/m");
    }
    
    /**
     * <p>Check if the subject with the supplied properties is a Freebase Topic</p>
     * @param properties The subject properties
     * @return boolean indicating if the subject is a topic or not
     */
    public static boolean isTopic(Map<String, List<String>> properties) {
        return (properties.containsKey(ImporterConstants.RDF_TYPE) && properties.get(ImporterConstants.RDF_TYPE).contains(ImporterConstants.FREEBASE_COMMON_TOPIC));
    }
}
