package uk.ac.ebi.ena.sra.pipeline.executors;

import uk.ac.ebi.ena.sra.pipeline.configuration.DefaultConfiguration;


public class 
LSFExecutorConfig implements ExecutorConfig
{
    public int getLSFMemoryReservationTimeout() { return DefaultConfiguration.CURRENT.getDefaultLSFMemTimeout(); }
    public int getLSFMemoryLimit() { return DefaultConfiguration.CURRENT.getDefaultLSFMem(); }
    public int getLSFCPUCores() { return DefaultConfiguration.CURRENT.getDefaultLSFCpuCores(); }
    public String getLsfUser() { return DefaultConfiguration.CURRENT.getDefaultLSFUser(); }
    public String getLsfQueue() { return DefaultConfiguration.CURRENT.getDefaultLSFQueue(); }
    public String getLsfOutputPath() { return DefaultConfiguration.CURRENT.getDefaultLSFOutputRedirection(); }
}