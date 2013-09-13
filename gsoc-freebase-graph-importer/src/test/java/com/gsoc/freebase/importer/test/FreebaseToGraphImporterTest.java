package com.gsoc.freebase.importer.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.URL;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.gsoc.freebase.importer.impl.FreebaseToGraphImporter;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;

/**
 * <p>
 * Class to tests the freebase to graph importer
 * </p>
 * 
 * @author Antonio David Perez Morales <adperezmorales@gmail.com>
 */
public class FreebaseToGraphImporterTest
{

    private static final String testFile = "test.nt.gz";
    private static final String testGraphDirectory = "/tmp/ftgitest" + UUID.randomUUID();
    private static File tempGraphDir;
    private static Graph graph;
    private static FreebaseToGraphImporter fbImporter;

    @BeforeClass
    public static void oneTimeSetUp()
    {
        System.out.println("Preparing FreebaseTpGraphImporterTest tests");
        URL url = FreebaseToGraphImporterTest.class.getClassLoader().getResource(testFile);
        tempGraphDir = new File(testGraphDirectory);
        if (tempGraphDir.exists())
        {
            System.out.println("Deleting directory " + testGraphDirectory);
            if (!tempGraphDir.delete())
                System.out.println("Error deleting directory");
        }

        // graph = new Neo4jGraph(testGraphDirectory);
        fbImporter = new FreebaseToGraphImporter(url.getFile(), testGraphDirectory);
    }

    @AfterClass
    public static void oneTimeTearDown()
    {
        System.out.println("Finishing FreebaseToGraphImporterTest tests");
        if (!tempGraphDir.delete())
            System.out.println("Error deleting the tmp directory");

        graph.shutdown();
    }

    /**
     * <p>
     * Test if the parser process is correct
     * </p>
     */
    @Test
    public void testProcessFile()
    {

        System.out.println("Process Test File");
        fbImporter.setGenerateGraph(true);
        fbImporter.setGenerateGraphRelations(true);
        fbImporter.process();

        graph = new Neo4jGraph(testGraphDirectory);
        assertTrue(graph.getVertices().iterator().hasNext());
        graph.shutdown();
    }

    /**
     * <p>
     * Test the number of vertices
     * </p>
     */
    @Test
    public void testVertices()
    {
        int i = 0;
        graph = new Neo4jGraph(testGraphDirectory);
        for (Vertex v : graph.getVertices())
            i++;

        assertEquals(546, i);
        graph.shutdown();
    }

    /**
     * <p>
     * Test the number of edges
     * </p>
     */
    @Test
    public void testEdges()
    {
        int i = 0;
        graph = new Neo4jGraph(testGraphDirectory);
        for (Edge e : graph.getEdges())
            i++;

        assertEquals(2, i);
        graph.shutdown();
    }
}
