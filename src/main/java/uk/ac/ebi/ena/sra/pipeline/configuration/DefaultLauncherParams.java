package uk.ac.ebi.ena.sra.pipeline.configuration;

import com.beust.jcommander.Parameter;

public class
DefaultLauncherParams
{
    @Parameter( names = "--workers", description = "number of simultaniously working processes" )
    public int workers = 2;
    
    @Parameter( names = "--lock", description = "lock file path" ) 
    public String lock = "/var/tmp/.launcher.lock";

    @Parameter( names = "--memory-limit", description = "memory per stage" )
    public int lsf_mem = DefaultConfiguration.currentSet().getDefaultLSFMem();

    @Parameter( names = "--cpu-cores-limit", description = "CPU cores per stage" )
    public int lsf_cpu_cores = DefaultConfiguration.currentSet().getDefaultLSFCpuCores();

    @Parameter( names = "--queue", description = "LSF queue name" )
    public String queue_name = DefaultConfiguration.currentSet().getDefaultLSFQueue();

    @Parameter( names = "--log-file", description = "log file" )
    public String log_file = "/var/tmp/launcher.log";

    @Parameter( names = "--lsf-mem-timeout", description = "timeout in minutes for lsf memory reservation" )
    public int lsf_mem_timeout = DefaultConfiguration.currentSet().getDefaultLSFMemTimeout();

}