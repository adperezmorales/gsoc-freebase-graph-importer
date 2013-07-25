package com.gsoc.freebase.importer.impl.step;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsoc.freebase.importer.ImporterConstants;
import com.gsoc.freebase.importer.impl.AbstractFreebaseImporterStep;
import com.gsoc.freebase.importer.utils.FreebaseUtils;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;

/**
 * <p>
 * Class representing an import step
 * </p>
 * <p>
 * It is responsible of create the graph relations (Edges) and add properties in the relations
 * </p>
 * <p>It creates an key index in the URI property of the vertex, to speed up the process of query vertices in the graph</p>
 * 
 * @author Antonio David Perez Morales <adperezmorales@gmail.com>
 * 
 */
public class FreebaseGenerateGraphRelationsStep extends AbstractFreebaseImporterStep
{
    /**
     * The default order for this step
     */
    public static final int DEFAULT_STEP_ORDER = 2;

    private static Logger logger = LoggerFactory.getLogger(FreebaseGenerateGraphRelationsStep.class);

    /**
     * The graph used
     */
    private Graph graph;

    int i;
    
    /**
     * <p>Default constructor</p>
     * <p>Sets the graph to be used in this step</p>
     * @param graph
     */
    public FreebaseGenerateGraphRelationsStep(Graph graph)
    {
        this.graph = graph;
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
     * Create the graph relations using the next algorithm:
     * <ul>
     * <li>If the entity is a topic, search the properties to see if there are properties which references (single
     * value) to other topics and create a direct relation between them (same 'multivalue' property referencing several
     * entities is ignored)</li>
     * <li>If the entity is not a topic, search the properties to see if there are properties which references (single
     * value) to other topics and create a mediated relation.</li>
     * <ul>
     * <p>
     * A direct relation creates an edge between topic vertex and the predicate referencing them is set as edge property
     * using the next rule: Property: music.recording.canonical_version Edge properties: music = music + 1
     * music.recording = music.recording + 1 music.recording.canonical_version = music.recording.canonical_version + 1;
     * </p>
     * <p>
     * A mediated relation creates edges between the topics which a non topic entity is referencing
     * </p>
     * <p>
     * Topic 1 <---- predicate --- {NotTopicMediator} --- predicate2 ----> Topic 2
     * </p>
     * <p>
     * For example: - [NotTopic predicate.ns.v Topic1] - [NotTopic other_predicate.ons.z Topic2] - [NotTopic pr.edi.cate
     * Topic3]
     * 
     * This example creates the following edges: - Topic1 <-----> Topic2 with properties: predicate.ns ,
     * other_predicate.ons.z (using the same rules as direct relations) - Topic1 <-----> Topic3 with properties:
     * predicate.ns , pr.edi.cate - Topic2 <-----> Topic3 with properties: other_predicate.ons.z , pr.edi.cate
     * </p>
     */
    @Override
    public void onItemRead(String subject, Map<String, List<String>> properties)
    {
        if (FreebaseUtils.isTopic(properties))
        {
            this.processTopic(subject, properties);
        }

        else
        {
            this.processNotTopic(subject, properties);
        }
    }

    /**
     * <p>
     * Process a topic entity
     * </p>
     * 
     * @param subject the entity subject
     * @param properties the entity properties
     */
    private void processTopic(String subject, Map<String, List<String>> properties)
    {
        for (String property : properties.keySet())
        {
            List<String> list = properties.get(property);
            if (list.size() > 1)
            {
                // Ignore multivalued properties
                continue;
            }

            String value = list.get(0);

            if (FreebaseUtils.isFreebaseId(value))

                if (this.isTopicGraph(value))
                {

                    // Create direct relation
                    this.createDirectRelation(property, subject, value);
                }

        }

    }

    /**
     * <p>
     * Process a not topic entity
     * </p>
     * 
     * @param subject the entity subject
     * @param properties the entity properties
     */
    private void processNotTopic(String subject, Map<String, List<String>> properties)
    {
        // Create mediated relations
        List<Holder> prevRelatedSubjects = new ArrayList<Holder>();
        for (String property : properties.keySet())
        {

            List<String> list = properties.get(property);
            if (list.size() > 1)
            {
                // Ignore multivalued properties
                continue;
            }

            String value = list.get(0);

            if (FreebaseUtils.isFreebaseId(value))
            {
                if (this.isTopicGraph(value))
                {
                    Holder holder = new Holder();
                    holder.property = property;
                    holder.uri = value;
                    if (prevRelatedSubjects.size() > 0)
                    {
                        this.createMediatedDirectRelation(subject, prevRelatedSubjects, holder);
                    }

                    prevRelatedSubjects.add(holder);
                }

            }
        }
    }

    /**
     * <p>
     * Check if the subject is a topic
     * </p>
     * <p>
     * That is to say, checks if exists a vertex in the graph with the supplied uri property
     * </p>
     * 
     * @param subject the subject to check
     * @return a boolean indicating if the subject is a topic or not
     */
    private boolean isTopicGraph(String subject)
    {
        return this.graph.getVertices(ImporterConstants.VERTEX_ENTITY_URI_PROPERTY, subject).iterator().hasNext();
    }

    /**
     * <p>
     * Called when the import of a file or input stream starts
     * </p>
     */
    @Override
    public void onImportStart()
    {
        logger.info("Starting Step " + this.getClass().getName() + " at " + System.currentTimeMillis());
        if(this.graph.getFeatures().supportsKeyIndices) {
            KeyIndexableGraph kig = (KeyIndexableGraph) this.graph;
            kig.createKeyIndex(ImporterConstants.VERTEX_ENTITY_URI_PROPERTY, Vertex.class);
        }
    }

    @Override
    public void onImportEnd()
    {
        ((Neo4jGraph)graph).commit();
        logger.info("Ending Step " + this.getClass().getName() + " at " + System.currentTimeMillis());
    }

    /**
     * <p>
     * Create mediated relations between topics
     * </p>
     * 
     * @param noTopicSubject the entity subject (not topic) acting as a mediator of other entities
     * @param prevRelatedSubjects the list of previous entities which will be related to the current topic entity
     * @param holder the current entity to be related with the previous ones
     */
    private void createMediatedDirectRelation(String noTopicSubject, List<Holder> prevRelatedSubjects, Holder holder)
    {
        Vertex n = graph.getVertices(ImporterConstants.VERTEX_ENTITY_URI_PROPERTY, holder.uri).iterator().next();

        for (Holder prevHolder : prevRelatedSubjects)
        {
            Vertex v = graph.getVertices(ImporterConstants.VERTEX_ENTITY_URI_PROPERTY, prevHolder.uri).iterator().next();
            Edge e = this.getEdgeBetweenVertices(v, n, ImporterConstants.MEDIATED_CONNECTION_EDGE_LABEL);
            if (e == null)
            {
                e = graph.addEdge(null, n, v, ImporterConstants.MEDIATED_CONNECTION_EDGE_LABEL);
            }

            e.setProperty("connected-by", noTopicSubject);

            this.updateEdgeValues(e, holder.property);
            this.updateEdgeValues(e, prevHolder.property);

        }

    }

    /**
     * <p>
     * Creating direct relation between topics
     * </p>
     * 
     * @param property the property linking the subjects
     * @param inSubject one subject to be related
     * @param outSubject the other subject to be related
     */
    private void createDirectRelation(String property, String inSubject, String outSubject)
    {
        Vertex out = graph.getVertices(ImporterConstants.VERTEX_ENTITY_URI_PROPERTY, inSubject).iterator().next();
        Vertex in = graph.getVertices(ImporterConstants.VERTEX_ENTITY_URI_PROPERTY, outSubject).iterator().next();

        Edge e = this.getEdgeBetweenVertices(in, out, ImporterConstants.DIRECT_CONNECTION_EDGE_LABEL);
        if (e == null)
        {
            e = graph.addEdge(null, out, in, ImporterConstants.DIRECT_CONNECTION_EDGE_LABEL);
        }

        this.updateEdgeValues(e, property);
    }

    /**
     * <p>
     * Get an existing edge between two vertices
     * </p>
     * 
     * @param in one vertex
     * @param out other vertex
     * @param label the edge label
     * @return the {@code Edge} connecting the two vertex or null if the edge doesn't exist
     */
    private Edge getEdgeBetweenVertices(Vertex in, Vertex out, String label)
    {
        Iterable<Edge> edges = out.getEdges(Direction.BOTH, label);
        for (Edge edge : edges)
        {
            if (edge.getVertex(Direction.IN).equals(in) || edge.getVertex(Direction.OUT).equals(in))
                return edge;
        }
        return null;
    }

    /**
     * <p>
     * Update the values of a property in an edge
     * </p>
     * <p>
     * If the property is of the form of a.b.c then a, a.b and a.b.c will be updated. That is to say, to add one to the
     * value of that property in the edge or creating a new property in the edge with value of 1
     * </p>
     * 
     * @param e the edge
     * @param fullProperty the property to be updated
     */
    private void updateEdgeValues(Edge e, String fullProperty)
    {
        String prop = fullProperty.substring(fullProperty.lastIndexOf("/") + 1);
        String[] splitProp = prop.split("\\.");
        String last = "";
        for (String s : splitProp)
        {
            this.updateEdgeValue(e, last + s);
            last += s + ".";
        }
    }

    /**
     * <p>
     * Update the value of a property in the edge, adding one to the value if the property exists in the edge or
     * creating a new one with value of 1
     * </p>
     * 
     * @param e the edge
     * @param s the property to update
     */
    private void updateEdgeValue(Edge e, String s)
    {
        Object o = e.getProperty(s);
        int i;
        if (o == null)
            i = 1;
        else
        {
            i = (int) o;
            i++;
        }

        e.setProperty(s, i);
    }

    /**
     * <p>
     * Class to represents a Holder, containing information about a subject and a property
     * </p>
     * <p>
     * It is used in mediated-connections when building all possible relations between topic entities connected by a not
     * topic entity
     * 
     * @author Antonio David Perez Morales <adperezmorales@gmail.com>
     * 
     */
    private static class Holder
    {
        public String property;
        public String uri;
    }

}
