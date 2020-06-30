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
    private int memory_limit;
    private int cpu_cores;
    private int lsf_mem_timeout;
    private int redo;
    
    
    public 
    DefaultExecutorFactory(  String pipeline_name, 
                             ResultTranslator translator, 
                             String queue,
                             int memory_limit,
                             int cpu_cores,
                             int lsf_mem_timeout,
                             int redo )
    {
        LSFQueue.findByName( queue );
        
        this.pipeline_name = pipeline_name; 
        this.translator = translator; 
        this.queue = queue;
        this.memory_limit = memory_limit;
        this.cpu_cores = cpu_cores;
        this.lsf_mem_timeout = lsf_mem_timeout;
        this.redo = redo;
    
    }
    
    public StageExecutor
    getExecutor()
    {
        LSFExecutorConfig cfg_def = new LSFExecutorConfig() {
            @Override public int getLSFMemoryReservationTimeout() { return lsf_mem_timeout; }
            @Override public String getLsfQueue() { return queue; }
        };

        StageExecutor executor = new LSFStageExecutor( pipeline_name, 
                                                       translator,
                                                       memory_limit,
                                                       cpu_cores,
                                                       cfg_def ).setRedoCount( redo );
        executor.setClientCanCommit( true );
        return executor;
    }
}
