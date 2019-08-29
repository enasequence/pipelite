package uk.ac.ebi.ena.sra.pipeline.configuration;

import uk.ac.ebi.ena.sra.pipeline.base.external.LSFClusterCall.LSFQueue;
import uk.ac.ebi.ena.sra.pipeline.executors.LSFExecutorConfig;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncher.StageExecutorFactory;
import uk.ac.ebi.ena.sra.pipeline.launcher.LSFStageExecutor;
import uk.ac.ebi.ena.sra.pipeline.launcher.ResultTranslator;
import uk.ac.ebi.ena.sra.pipeline.launcher.StageExecutor;

public class 
DefaultExecutorFactory implements StageExecutorFactory
{
    private String pipeline_name;
    private ResultTranslator translator; 
    private String queue;
    private String lsf_user;
    private int lsf_mem;
    private int lsf_mem_timeout;
    private int lsf_cpu_cores;
    private int redo;
    
    
    public 
    DefaultExecutorFactory(  String pipeline_name, 
                             ResultTranslator translator, 
                             String queue,
                             String lsf_user,
                             int lsf_mem, 
                             int lsf_mem_timeout,
                             int lsf_cpu_cores,
                             int redo )
    {
        LSFQueue.findByName( queue );
        
        this.pipeline_name = pipeline_name; 
        this.translator = translator; 
        this.queue = queue;
        this.lsf_user = lsf_user;
        this.lsf_mem = lsf_mem; 
        this.lsf_mem_timeout = lsf_mem_timeout;
        this.lsf_cpu_cores = lsf_cpu_cores; 
        this.redo = redo;
    
    }
    
    public StageExecutor
    getExecutor()
    {
        LSFExecutorConfig cfg_def = new LSFExecutorConfig() {
            @Override public int getLSFMemoryLimit() { return  lsf_mem; }
            @Override public int getLSFMemoryReservationTimeout() { return lsf_mem_timeout; }
            @Override public int getLSFCPUCores() { return  lsf_cpu_cores; }
            @Override public String getLsfUser() { return lsf_user; }
            @Override public String getLsfQueue() { return queue; }
        };

        StageExecutor executor = new LSFStageExecutor( pipeline_name, 
                                                       translator, 
                                                       cfg_def ).setRedoCount( redo );
        executor.setClientCanCommit( true );
        return executor;
    }
}
