package com.gsoc.freebase.importer.producer.impl;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jena.atlas.lib.Tuple;
import org.apache.jena.riot.system.StreamRDF;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

import com.hp.hpl.jena.datatypes.BaseDatatype;
import com.hp.hpl.jena.datatypes.BaseDatatype.TypedValue;
import com.hp.hpl.jena.datatypes.xsd.XSDDateTime;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.Quad;

/**
 * <p>
 * AbstractProducerImporterHandler class
 * </p>
 * <p>
 * Abstract (Stream RDF) class to be used with Riot Reader to listen read triples
 * </p>
 * 
 * @author Antonio David Perez Morales <adperezmorales@gmail.com>
 * 
 */
public abstract class AbstractProducerImporterHandler implements StreamRDF
{

    /**
     * The previous subject processed
     */
    private String previousSubject;

    /**
     * The properties of the subject being processed
     */
    private Map<String, List<String>> properties;

    /**
     * The factory to generate and convert values
     */
    ValueFactory valueFactory = ValueFactoryImpl.getInstance();

    /**
     * <p>
     * Called when the parser starts
     * </p>
     */
    @Override
    public void start()
    {
        /*
         * Init variables
         */
        this.previousSubject = "";
        this.properties = new HashMap<String, List<String>>();

        // Simply call to onImportStart, which will be override by the child classes
        this.onImportStart();
    }

    /**
     * <p>
     * Called when a Triple is processed
     * </p>
     */
    @Override
    public void triple(Triple triple)
    {

        if (!previousSubject.equals(triple.getSubject().getURI()))
        {
            if (previousSubject != null && previousSubject != "")
            {
                /*
                 * Call the onItemRead whose implementation is in the child classes
                 */
                this.onItemRead(previousSubject, properties);
            }

            previousSubject = triple.getSubject().getURI();
            this.properties = new HashMap<String, List<String>>();
        }

        Node predicate = triple.getPredicate();
        Node object = triple.getObject();

        String objectValue = this.generateStringValue(object);

        if (properties.get(predicate.getURI()) == null)
            properties.put(predicate.getURI(), new ArrayList<String>());

        properties.get(predicate.getURI()).add(objectValue);
    }

    @Override
    public void quad(Quad quad)
    {
        return;
    }

    @Override
    public void tuple(Tuple<Node> tuple)
    {
        return;
    }

    @Override
    public void base(String base)
    {
        return;
    }

    @Override
    public void prefix(String prefix, String iri)
    {
        return;
    }

    @Override
    public void finish()
    {
        // Store the last entity identified by previousSubject and properties
        this.onItemRead(previousSubject, properties);
        this.onImportEnd();
    }

    /**
     * <p>
     * Generate a String representation of the Node object
     * </p>
     * 
     * @param object the Node to obtain the string representation
     * @return the {@code String} representation
     */
    private String generateStringValue(Node object)
    {

        Value objectValue = null;

        if (object.isURI())
        {
            objectValue = valueFactory.createURI(object.getURI());
        }
        else if (object.isLiteral())
        {
            Object obj = object.getLiteralValue();
            if (obj instanceof Boolean)
            {
                objectValue = valueFactory.createLiteral((Boolean) obj);
            }
            else if (obj instanceof Integer)
            {
                objectValue = valueFactory.createLiteral((Integer) obj);
            }
            else if (obj instanceof Long)
            {
                objectValue = valueFactory.createLiteral((Long) obj);
            }
            else if (obj instanceof String)
            {
                objectValue = valueFactory.createLiteral((String) obj);
            }
            else if (obj instanceof Float)
            {
                objectValue = valueFactory.createLiteral((Float) obj);
            }
            else if (obj instanceof Double)
            {
                objectValue = valueFactory.createLiteral((Double) obj);
            }
            else if (obj instanceof BigDecimal)
            {
                BigDecimal bd = (BigDecimal) obj;
                objectValue = valueFactory.createLiteral(bd.toPlainString());
            }
            else if (obj instanceof BaseDatatype.TypedValue)
            {
                TypedValue val = (TypedValue) obj;
                objectValue = valueFactory.createLiteral(val.lexicalValue);
            }
            else if (obj instanceof XSDDateTime)
            {
                XSDDateTime date = (XSDDateTime) obj;
                objectValue = valueFactory.createLiteral(date.toString());
            }
        }
        return objectValue.stringValue();
    }

    /**
     * <p>
     * Method called when a new item (entity) has been read
     * </p>
     * 
     * @param subject the subject (URI) of the item
     * @param properties the properties of the item
     */
    public abstract void onItemRead(String subject, Map<String, List<String>> properties);

    /**
     * <p>
     * Method called when the import process starts. Redefine it in child classes
     * </p>
     */
    public void onImportStart(){}

    /**
     * <p>
     * Method called when the import process ends. Redefine it in child classes
     * </p>
     */
    public void onImportEnd(){}

}
