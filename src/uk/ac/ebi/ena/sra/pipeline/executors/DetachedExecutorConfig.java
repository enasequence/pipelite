package uk.ac.ebi.ena.sra.pipeline.executors;


public interface 
DetachedExecutorConfig extends ExecutorConfig
{
    default public int getJavaMemoryLimit() { return -1; }
}
