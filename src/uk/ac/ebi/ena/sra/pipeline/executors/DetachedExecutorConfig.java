package uk.ac.ebi.ena.sra.pipeline.executors;

import uk.ac.ebi.ena.sra.pipeline.configuration.DefaultConfiguration;


public class
DetachedExecutorConfig implements ExecutorConfig
{
    public int getJavaMemoryLimit() { return -1; }
    public String[] getPropertiesPass() { return DefaultConfiguration.CURRENT.getPropertiesPass(); }
}
