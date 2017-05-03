package uk.ac.ebi.ena.sra.pipeline.launcher;

import java.util.ArrayList;
import java.util.List;

import uk.ac.ebi.ena.sra.pipeline.base.external.ExternalCall;
import uk.ac.ebi.ena.sra.pipeline.base.external.ExternalCallException;
import uk.ac.ebi.ena.sra.pipeline.configuration.DefaultConfiguration;
import uk.ac.ebi.ena.sra.pipeline.executors.DetachedExecutorConfig;
import uk.ac.ebi.ena.sra.pipeline.executors.ExecutorConfig;

public class 
DetachedStageExecutor extends AbstractStageExecutor implements DetachedExecutorConfig
{
    private static final String DEFAULT_EXECUTION_ID = "DetachedStageExecutor";
    private boolean do_commit = true;
    private boolean was_error;
    ExecutionInfo   info;
    protected ExternalCallBackEnd back_end = new SimpleBackEnd();
    private   ExecutorConfig rc = new DetachedExecutorConfig() {};    
    
    
    public
    DetachedStageExecutor( String pipeline_name, ResultTranslator translator )
    {
        super( pipeline_name, translator );
    }
    
    
    public void
    reset( StageInstance instance )
    {
        instance.setExecutionInstance( new ExecutionInstance() );
    }
    
    
    private List<String> 
    constructArgs( StageInstance instance, boolean commit )
    {
        List<String>p_args = new ArrayList<String>();
        
        int memory_limit = getJavaMemoryLimit();
        if( 0 < memory_limit ) // TODO check
            p_args.add( String.format( "-Xmx%dM", memory_limit ) );
        
        p_args.add( String.format( "-D%s=%s", 
                                   DefaultConfiguration.currentSet().getConfigPrefixName(), 
                                   DefaultConfiguration.currentSet().getConfigSourceName() ) ); 
        
        p_args.add( "-cp" );
        p_args.add( System.getProperty( "java.class.path" ) ); 
        p_args.add( StageLauncher.class.getName() );
        
        p_args.add( uk.ac.ebi.ena.sra.pipeline.launcher.StageLauncher.PARAMETERS_NAME_ID );
        p_args.add( instance.getProcessID() );
    
        p_args.add( uk.ac.ebi.ena.sra.pipeline.launcher.StageLauncher.PARAMETERS_NAME_STAGE );
        p_args.add( instance.getStageName() );
    
        if( commit )
            p_args.add( uk.ac.ebi.ena.sra.pipeline.launcher.StageLauncher.PARAMETERS_NAME_FORCE_COMMIT );
        
        return p_args;
    }
    
    
    public void 
    execute( StageInstance instance )
    {
        if( EvalResult.StageTransient == can_execute( instance ) )
        {
            log.info( String.format( "%sxecuting stage %s", 0 == instance.getExecutionCount() ? "E" : "Re-e", instance.getStageName() ) );

            List<String> p_args = constructArgs( instance, do_commit );
            ExternalCall ec     = back_end.new_call_instance( String.format( "%s~%s~%s", 
                                                                             PIPELINE_NAME, //TODO: get from instance 
                                                                             instance.getProcessID(),
                                                                             instance.getStageName() ), 
                                                              "java", 
                                                              p_args.toArray( new String[ p_args.size() ] ) );
            log.info( ec.getCommandLine() );

            ec.execute();
                
            fillExecutionInfo( ec );
            
            String print_msg = String.format( "Finished execution of stage %s\n%s", 
                                              instance.getStageName(),
                                              new ExternalCallException( ec ).toString() );

            log.info( print_msg );
        }
    }


    private void
    fillExecutionInfo( ExternalCall ec )
    {
        info = new ExecutionInfo();
        info.setCommandline( ec.getCommandLine() );
        info.setStdout( ec.getStdout() );
        info.setStderr( ec.getStderr() );
        info.setExitCode( Integer.valueOf( ec.getExitCode() ) );
        info.setHost( ec.getHost() );
        info.setPID( ec.getPID() );
        info.setThrowable( new ExternalCallException( ec ) );
    }

    
    
    @Override public ExecutionInfo
    get_info()
    {
        return info;
    }
    
    
    @Override public void 
    setClientCanCommit( boolean do_commit )
    {
        this.do_commit = do_commit;
    }
    
    
    @Override public boolean 
    getClientCanCommit()
    {
        return this.do_commit;
    }

    
    @Override public void
    configure( ExecutorConfig rc )
    {
        this.rc = rc;
    }
    
    
    @Override public int 
    getJavaMemoryLimit() 
    { 
        return ((DetachedExecutorConfig)rc).getJavaMemoryLimit(); 
    }
}
