package com.gsoc.freebase.importer.main;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import com.gsoc.freebase.importer.Importer;
import com.gsoc.freebase.importer.ImporterStep;
import com.gsoc.freebase.importer.impl.FreebaseToGraphImporter;
import com.gsoc.freebase.importer.impl.step.FreebaseGenerateGraphRelationsStep;
import com.gsoc.freebase.importer.impl.step.FreebaseGenerateGraphStep;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;

/**
 * <p>
 * Main class to import BaseKBLime Freebase dump into a graph
 * </p>
 * 
 * @author Antonio David Perez Morales <adperezmorales@gmail.com>
 * 
 */
public class Main
{
    public static final String JAR_NAME = "freebase-graph-importer-{*}-jar-with-dependencies.jar";
    private static final Options options;
    static
    {
        options = new Options();
        options.addOption("h", "help", false, "Display this help and exit");
        options.addOption("i", "inputDirectory", true,
                "The BaseKBLime Freebase dataset file to be processed or the input directory containing the Freebase dataset files");
        options.addOption("o", "outputDirectory", true, "The output directory where the graph wil be generated");
        options.addOption("g", "generateGraph", false, "Tell the importer to generate the graph structure (one vertex for each entity which is a common.topic)");
        options.addOption("r", "generateRelations", false, "Tell the importer to generate the graph relations (edges between entities directly or indirectly connected");

    }

    /**
     * @param args
     * @throws ParseException
     */
    public static void main(String[] args) throws ParseException
    {
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("h") || args.length <= 0)
        {
            printHelp();
            System.exit(0);
        }

        if (!cmd.hasOption("i") && cmd.getOptionValue("i").equals(""))
        {
            System.out.println("The parameter i is missing or the value is empty");
            System.exit(0);
        }

        if (!cmd.hasOption("o") && cmd.getOptionValue("o").equals(""))
        {
            System.out.println("The parameter o is missing or the value is empty");
            System.exit(0);
        }

        String inputDirectory = cmd.getOptionValue("i");
        String outputDirectory = cmd.getOptionValue("o");

        // Checking if the inputs are directories
        File inputDir = new File(inputDirectory);
        File outputDir = new File(outputDirectory);

        if (outputDir.exists() && outputDir.isFile())
        {
            System.out.println(outputDirectory + " is a file. Please enter a valid directory to store the graph");
            System.exit(0);
        }

        if (!inputDir.exists())
        {
            System.out.println(inputDirectory + " doesn't exist. Please enter a valid file or directory to be processed");
            System.exit(0);
        }

        Neo4jGraph graph = new Neo4jGraph(outputDirectory);
        
        Importer importer = new FreebaseToGraphImporter(inputDir);
        if(options.hasOption("g")) {
            ImporterStep step = new FreebaseGenerateGraphStep(graph);
            importer.addStep(step);
        }
        
        if(options.hasOption("r")) {
            ImporterStep step = new FreebaseGenerateGraphRelationsStep(graph);
            importer.addStep(step);
        }
       
        /* Run the parser process */
        System.out.println("Starting the process...");
        long start = System.currentTimeMillis();
        importer.process();
        /* Closing the graph */
        graph.shutdown();
        
        long end = System.currentTimeMillis();
        
        System.out.println("Import process finished");
        System.out.println("Duration: "+((end-start)/1000)+" seconds");
        System.exit(0);
    }

    /**
     * <p>
     * Print the help
     * </p>
     */
    private static void printHelp()
    {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("java -Xmx{size} -jar " + JAR_NAME + " [options] \n",
                "Freebase to Graph Importer Tool\n", options, null);
    }

}
