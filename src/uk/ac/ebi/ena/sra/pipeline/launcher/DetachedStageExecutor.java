package uk.ac.ebi.ena.sra.pipeline.launcher;

import java.util.ArrayList;
import java.util.List;

import uk.ac.ebi.ena.sra.pipeline.base.external.ExternalCall;
import uk.ac.ebi.ena.sra.pipeline.base.external.ExternalCallException;
import uk.ac.ebi.ena.sra.pipeline.configuration.DefaultConfiguration;
import uk.ac.ebi.ena.sra.pipeline.executors.DetachedExecutorConfig;
import uk.ac.ebi.ena.sra.pipeline.executors.ExecutorConfig;

public class 
DetachedStageExecutor extends AbstractStageExecutor
{
    private static final String DEFAULT_EXECUTION_ID = "DetachedStageExecutor";
    private boolean do_commit = true;
    private String   config_prefix_name;
    private String   config_source_name;
    private int memory_limit;
    private boolean was_error;
    ExecutionInfo   info;
    protected ExternalCallBackEnd back_end = new SimpleBackEnd();
    private String[] properties_pass;
    
    public
    DetachedStageExecutor( String pipeline_name, ResultTranslator translator )
    {
        this( pipeline_name, translator,
            DefaultConfiguration.currentSet().getConfigPrefixName(),
            DefaultConfiguration.currentSet().getConfigSourceName(),
            DefaultConfiguration.CURRENT.getPropertiesPass() );
    }


    public
    DetachedStageExecutor( String pipeline_name, ResultTranslator translator, String config_prefix_name, String config_source_name, String[] properties_pass )
    {
        super( pipeline_name, translator );
        this.config_prefix_name = config_prefix_name;
        this.config_source_name = config_source_name;
        this.properties_pass = properties_pass;
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
        DetachedExecutorConfig si_config = instance.getResourceConfig( DetachedExecutorConfig.class );
        if( null != si_config )
            memory_limit = si_config.getJavaMemoryLimit();

        if( 0 < memory_limit ) // TODO check
            p_args.add( String.format( "-Xmx%dM", memory_limit ) );
        
        p_args.add( String.format( "-D%s=%s", config_prefix_name, config_source_name ) );
        
        appendProperties( p_args, getPropertiesPass() );
        
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

    @Override
    public Class<? extends ExecutorConfig> getConfigClass() {
        return DetachedExecutorConfig.class;
    }


    @Override public void
    configure( ExecutorConfig rc )
    {
        DetachedExecutorConfig params = (DetachedExecutorConfig)rc;
        if( null != params )
        {
            memory_limit = params.getJavaMemoryLimit();
        }
    }
    
    
    public int
    getJavaMemoryLimit() 
    { 
        return memory_limit;
    }
    
    
    //TODO looks overengineered
    public String[]
    getPropertiesPass()
    {
        return properties_pass;
    }
}
