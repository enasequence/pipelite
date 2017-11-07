package uk.ac.ebi.ena.sra.pipeline.executors;

import uk.ac.ebi.ena.sra.pipeline.configuration.DefaultConfiguration;


public interface 
DetachedExecutorConfig extends ExecutorConfig
{
    default public int getJavaMemoryLimit() { return -1; }
    default public String[] getPropertiesPass() { return DefaultConfiguration.CURRENT.getPropertiesPass(); }
}
