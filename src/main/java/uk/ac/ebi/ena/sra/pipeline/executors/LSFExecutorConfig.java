package uk.ac.ebi.ena.sra.pipeline.executors;

import uk.ac.ebi.ena.sra.pipeline.configuration.DefaultConfiguration;


public class 
LSFExecutorConfig implements ExecutorConfig
{
    public int getLSFMemoryReservationTimeout() { return DefaultConfiguration.CURRENT.getDefaultLSFMemTimeout(); }
    public String getLsfQueue() { return DefaultConfiguration.CURRENT.getDefaultLSFQueue(); }
    public String getLsfOutputPath() { return DefaultConfiguration.CURRENT.getDefaultLSFOutputRedirection(); }
}