package com.gsoc.freebase.importer.impl.step;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsoc.freebase.importer.ImporterConstants;
import com.gsoc.freebase.importer.impl.AbstractFreebaseImporterStep;
import com.gsoc.freebase.importer.utils.FreebaseUtils;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;

/**
 * <p>
 * Class representing an import step
 * </p>
 * <p>
 * It is responsible of create the graph structure (Vertices)
 * </p>
 * 
 * @author Antonio David Perez Morales <adperezmorales@gmail.com>
 * 
 */
public class FreebaseGenerateGraphStep extends AbstractFreebaseImporterStep
{
    /**
     * Logger
     */
    private static Logger logger = LoggerFactory.getLogger(FreebaseGenerateGraphStep.class);
    /**
     * The default order for this step
     */
    public static final int DEFAULT_STEP_ORDER = 1;

    private static final String TYPE_OBJECT_NAME = ImporterConstants.FREEBASE_NAMESPACE + "type.object.name";

    /**
     * The graph used
     */
    private Graph graph;

    /**
     * <p>Default constructor</p>
     * <p>It sets the graph to be used</p>
     * @param graph
     */
    public FreebaseGenerateGraphStep(Graph graph)
    {
        this.graph = graph;
    }

    /**
     * <p>Sets the graph to be used</p>
     * @param graph
     */
    public void setGraph(Graph graph) {
        this.graph = graph;
    }
    
    /**
     * <p>Gets the graph being used</p>
     * @return
     */
    public Graph getGraph() {
        return this.graph;
    }
    /*
     * (non-Javadoc)
     * 
     * @see com.gsoc.freebase.importer.ImporterStep#getOrder()
     */
    public int getOrder()
    {
        return this.order == 0 ? DEFAULT_STEP_ORDER : this.order;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.gsoc.freebase.importer.ImporterStep#setOrder(int)
     */
    public void setOrder(int order)
    {
        this.order = order;

    }

    /**
     * <p>
     * Adds a new Vertex to the graph if the supplied subject is a topic
     * </p>
     */
    @Override
    public void onItemRead(String subject, Map<String, List<String>> properties)
    {
        if (FreebaseUtils.isTopic(properties))
        {
            Vertex vertex = graph.addVertex(null);
            vertex.setProperty(ImporterConstants.VERTEX_ENTITY_URI_PROPERTY, subject);

            /*
             * Adding only rdf-type and type.object.name properties
             */
            for (String property : properties.keySet())
            {
                if (property.equals(ImporterConstants.RDF_TYPE) || property.equals(TYPE_OBJECT_NAME))
                {
                    vertex.setProperty(property, properties.get(property));
                }
            }
        }
    }

    /**
     * <p>
     * Called when the import of a file or input stream starts
     * </p>
     */
    @Override
    public void onImportStart()
    {
        if(graph.getFeatures().supportsVertexKeyIndex) {
            KeyIndexableGraph keyGraph = (KeyIndexableGraph) this.graph;
            keyGraph.createKeyIndex(ImporterConstants.VERTEX_ENTITY_URI_PROPERTY, Vertex.class);
        }
        
        logger.info("Starting Step " + this.getClass().getName() + " at " + System.currentTimeMillis());

    }

    @Override
    public void onImportEnd()
    {
        if(graph.getFeatures().supportsTransactions) {
            TransactionalGraph transGraph = (TransactionalGraph) this.graph;
            transGraph.commit();
        }
        logger.info("Ending Step " + this.getClass().getName() + " at " + System.currentTimeMillis());
    }

}
