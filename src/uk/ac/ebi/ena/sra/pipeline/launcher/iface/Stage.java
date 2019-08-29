package uk.ac.ebi.ena.sra.pipeline.launcher.iface;

import uk.ac.ebi.ena.sra.pipeline.executors.ExecutorConfig;


public interface 
Stage
{
    public Class<? extends StageTask> getTaskClass();
    public Stage getDependsOn();
    public String getDescription();
    default public int getJavaMemoryLimit() { return -1; };
    default public String[] getPropertiesPass() { return new String[] {}; };
    default public ExecutorConfig[] getExecutorConfig() { return new ExecutorConfig[] {}; };
}
