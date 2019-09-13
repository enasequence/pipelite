package uk.ac.ebi.ena.sra.pipeline.launcher.iface;

import uk.ac.ebi.ena.sra.pipeline.executors.ExecutorConfig;


public interface 
Stage
{
    public Class<? extends StageTask> getTaskClass();
    public Stage getDependsOn();
    public String getDescription();
    default public int getMemoryLimit() { return -1; }
    default public int getCPUCores() { return 1; }
    default public String[] getPropertiesPass() { return new String[] {}; }
    default public ExecutorConfig[] getExecutorConfig() { return new ExecutorConfig[] {}; }
}
