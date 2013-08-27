package com.gsoc.freebase.importer.impl;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

import org.apache.jena.riot.RiotReader;
import org.apache.jena.riot.system.StreamRDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsoc.freebase.importer.Importer;
import com.gsoc.freebase.importer.ImporterStep;

/**
 * <p>
 * Freebase importer to a Graph using Riot parser
 * </p>
 * <p>
 * It parses the BaseKBLime Freebase data dump (in Turtle, RDF, NTriples format...) and store it in a graph
 * </p>
 * <p>
 * It uses files or directories to import into a Graph Database managed by Tinkerpop Blueprints
 * </p>
 * <p>
 * This importer creates a graph in several steps. This will process every file or directory for each step.
 * </p>
 * 
 * <p>
 * For example:
 * <ul>
 * <li>Create graph step: It will process every file or files (in the directory) to create the graph vertices
 * <li>Create graph relations step: It will process every file or files (in the directory) to create the graph
 * relations.
 * </ul>
 * </p>
 * <p>
 * In the previous example, the files are processed two times, one time for creating the graph structure and other time
 * for creating the relations and properties (kind of weights) in the edges
 * </p>
 * 
 * @author Antonio David Perez Morales <adperezmorales@gmail.com>
 * 
 */
public class FreebaseToGraphImporter implements Importer
{
    private static Logger logger = LoggerFactory.getLogger(FreebaseToGraphImporter.class);
    /**
     * <p>
     * Set containing the steps to execute
     * </p>
     */
    Set<ImporterStep> steps;

    /**
     * The file or directory to import
     */
    private File file;

    /**
     * <p>
     * Constructor
     * </p>
     * <p>
     * Creates an importer to process the file or directory passed as parameter
     * </p>
     * 
     * @param file the string or directory to import
     */
    public FreebaseToGraphImporter(String file)
    {
        this(new File(file));
    }

    /**
     * <p>
     * Constructor
     * </p>
     * <p>
     * Creates an importer to process the file or directory passed as parameter
     * </p>
     * <p>
     * Initializes the set of steps
     * </p>
     * 
     * @param file the string or directory to import
     */
    public FreebaseToGraphImporter(File file)
    {
        this.file = file;
        this.steps = new TreeSet<>(new Comparator<ImporterStep>()
        {

            @Override
            public int compare(ImporterStep o1, ImporterStep o2)
            {
                if (o1.getOrder() == o2.getOrder())
                    return 0;
                else if (o1.getOrder() > o2.getOrder())
                    return 1;
                else
                    return -1;
            }

        });
    }

    /**
     * <p>
     * Build an instance using the given file name and the steps
     * </p>
     */
    public FreebaseToGraphImporter(String file, Set<ImporterStep> steps)
    {
        this.file = new File(file);
        this.steps = steps;
    }

    /**
     * <p>
     * Add the step to the step list
     * </p>
     * <p>
     * It will be added in the execution order according to the result obtained from call the getOrder method
     * </p>
     * 
     * @param step the {@code ImporterStep} step to add
     */
    public void addStep(ImporterStep step)
    {
        this.steps.add(step);
    }

    /**
     * <p>
     * Get the configured steps
     * </p>
     * 
     * @return a {@code Set<ImporterStep>} containing the configured steps
     */
    public Set<ImporterStep> getSteps()
    {
        return this.steps;
    }

    /**
     * <p>
     * Performs the import process based on the supplied steps
     * </p>
     */
    @Override
    public void process()
    {
        // Finish if no steps
        if (this.steps.isEmpty())
        {
            logger.info("Steps empty. Skip process");
            return;
        }

        /*
         * Process the steps. This importer needs that the steps are instances of StreamRDF.
         * If not, the step is skipped
         */
        for (ImporterStep step : this.steps)
        {
            if(!(step instanceof StreamRDF)) {
                logger.debug("The step "+step.getClass().getName()+" doesn't implement "+StreamRDF.class.getName()+". Skipping the process");
                continue;
            }
            
            StreamRDF currentStep = (StreamRDF) step;
            
            if (file.isDirectory())
            {
                logger.debug(file.getAbsolutePath() + " is a directory. Processing directory files (one level only)");

                File[] files = file.listFiles(new FilenameFilter()
                {
                    @Override
                    public boolean accept(File dir, String name)
                    {
                        if (name.startsWith("."))
                            return false;
                        else
                            return true;
                    }
                });

                for (File f : files)
                {
                    this.processFile(f, currentStep);
                }
            }
            else
            {
                logger.debug("Processing file " + this.file.getAbsolutePath());
                this.processFile(this.file, currentStep);
            }
        }
    }

    /**
     * <p>
     * Process the file
     * </p>
     * <p>
     * In this case, a {@code RiotReader} object is used with the current step
     * </p>
     * 
     * @param f the file to process
     * @param step the current step. It is instance of {@code StreamRDF}
     */
    private void processFile(File f, StreamRDF step)
    {
        RiotReader.parse(f.getAbsolutePath(), step);
    }
}
