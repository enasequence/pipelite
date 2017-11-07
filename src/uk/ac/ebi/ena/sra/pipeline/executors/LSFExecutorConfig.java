package uk.ac.ebi.ena.sra.pipeline.executors;

import uk.ac.ebi.ena.sra.pipeline.configuration.DefaultConfiguration;


public interface 
LSFExecutorConfig extends ExecutorConfig
{
    default public int getLSFMemoryReservationTimeout() { return DefaultConfiguration.CURRENT.getDefaultLSFMemTimeout(); }
    default public int getLSFMemoryLimit() { return DefaultConfiguration.CURRENT.getDefaultLSFMem(); }
    default public int getLSFCPUCores() { return DefaultConfiguration.CURRENT.getDefaultLSFCpuCores(); }
    default public int getJavaMemoryLimit() { return getLSFMemoryLimit() - 1500; }
    default public String[] getPropertiesPass() { return DefaultConfiguration.CURRENT.getPropertiesPass(); }
}