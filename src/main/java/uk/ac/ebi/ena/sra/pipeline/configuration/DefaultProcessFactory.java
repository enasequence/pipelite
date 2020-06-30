package uk.ac.ebi.ena.sra.pipeline.configuration;

import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncher.PipeliteProcess;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncher.ProcessFactory;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncher.StageExecutorFactory;
import uk.ac.ebi.ena.sra.pipeline.launcher.ProcessLauncher;

public class 
DefaultProcessFactory implements ProcessFactory
{
    StageExecutorFactory executor_factory;
     
    
    public 
    DefaultProcessFactory()
    {
    }
    
    
    @Override public PipeliteProcess
    getProcess( String process_id )
    {
        ProcessLauncher process = new ProcessLauncher();
        process.setPipelineName( DefaultConfiguration.currentSet().getPipelineName() );
        process.setProcessID( process_id );
        process.setRedoCount( DefaultConfiguration.currentSet().getStagesRedoCount() );
        //TODO: locking routine should be changed
        //process.setExecutor( new DetachedStageExecutor( DefaultConfiguration.currentSet().getCommitStatus() ) );

        process.setStages( DefaultConfiguration.currentSet().getStages() );
        process.setCommitStatuses( DefaultConfiguration.currentSet().getCommitStatus() );
        return process;
    }
}
