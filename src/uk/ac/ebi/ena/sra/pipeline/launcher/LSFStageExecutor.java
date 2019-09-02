package uk.ac.ebi.ena.sra.pipeline.launcher;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import java.util.Set;
import uk.ac.ebi.ena.sra.pipeline.base.external.ExternalCall;
import uk.ac.ebi.ena.sra.pipeline.base.external.ExternalCallException;
import uk.ac.ebi.ena.sra.pipeline.base.external.LSFClusterCall;
import uk.ac.ebi.ena.sra.pipeline.configuration.DefaultConfiguration;
import uk.ac.ebi.ena.sra.pipeline.executors.ExecutorConfig;
import uk.ac.ebi.ena.sra.pipeline.executors.LSFExecutorConfig;
import uk.ac.ebi.ena.sra.pipeline.launcher.iface.ExecutionResult;


public class 
LSFStageExecutor extends AbstractStageExecutor
{
    private boolean  do_commit = true;
    ExecutionInfo    info;
    private String   config_prefix_name;
    private String   config_source_name;
    private ExecutionResult default_failure_result;
    private String[] properties_pass;
    LSFExecutorConfig config;


    public
    LSFStageExecutor( String pipeline_name, 
                      ResultTranslator translator,
                      LSFExecutorConfig config )
    {
    	this( pipeline_name, translator,
              DefaultConfiguration.CURRENT.getConfigPrefixName(),
              DefaultConfiguration.CURRENT.getConfigSourceName(),
              DefaultConfiguration.CURRENT.getPropertiesPass(),
              config );
    }
    
    
    LSFStageExecutor( String pipeline_name, 
                      ResultTranslator translator,
                      String config_prefix_name,
                      String config_source_name,
                      String[] properties_pass,
                      LSFExecutorConfig config )
    {
        super( pipeline_name, translator );
        this.default_failure_result = translator.getCommitStatusDefaultFailure();
        this.config_prefix_name = config_prefix_name;
        this.config_source_name = config_source_name;

        this.properties_pass = properties_pass;

        this.config = config;
    }
    
    public void
    reset( StageInstance instance )
    {
        instance.setExecutionInstance( new ExecutionInstance() );
    }
    
    
    public void
    configure( LSFExecutorConfig params )
    {
        if( null != params )
            this.config = params;
    }
    
    
    public String[]
    getPropertiesPass()
    {
    	 return properties_pass; 
    }


    private String []
    mergePropertiesPass( String [] pp1, String [] pp2 )
    {
        Set<String> set1 = new HashSet<>( Arrays.asList( pp1 ) );
        Set<String> set2 = new HashSet<>( Arrays.asList( pp2 ) );
        set1.addAll( set2 );
        return set1.toArray( new String[ set1.size() ] );
    }

    
    private List<String> 
    constructArgs( StageInstance instance, boolean commit )
    {
        List<String>p_args = new ArrayList<String>();


        p_args.add( "-XX:+UseSerialGC" );
     
        int memory_limit = instance.getJavaMemoryLimit();

        if( ( config.getLSFMemoryLimit() >= memory_limit + 1500 ) && ( 0 < memory_limit ) ) // TODO check
            p_args.add( String.format( "-Xmx%dM", memory_limit ) );
        
        p_args.add( String.format( "-D%s=%s", config_prefix_name, config_source_name ) );

        String [] prop_pass = mergePropertiesPass( getPropertiesPass(), instance.getPropertiesPass() );
        appendProperties( p_args, prop_pass );
        
        p_args.add( "-cp" );
        p_args.add( System.getProperty( "java.class.path" ) ); 
        p_args.add( StageLauncher.class.getName() );
        
        p_args.add( uk.ac.ebi.ena.sra.pipeline.launcher.StageLauncher.PARAMETERS_NAME_ID );
        p_args.add( instance.getProcessID() );
    
        p_args.add( uk.ac.ebi.ena.sra.pipeline.launcher.StageLauncher.PARAMETERS_NAME_STAGE );
        p_args.add( instance.getStageName() );
    
        if( commit )
            p_args.add( uk.ac.ebi.ena.sra.pipeline.launcher.StageLauncher.PARAMETERS_NAME_FORCE_COMMIT );
        
        p_args.add( uk.ac.ebi.ena.sra.pipeline.launcher.StageLauncher.PARAMETERS_NAME_ENABLED );
        p_args.add( Boolean.toString( instance.isEnabled() ).toLowerCase() );
        
        p_args.add( uk.ac.ebi.ena.sra.pipeline.launcher.StageLauncher.PARAMETERS_NAME_EXEC_COUNT );
        p_args.add( Long.toString( instance.getExecutionCount() ) );
        
        return p_args;
    }


    private LSFBackEnd
    configureBackend()
    {
        LSFBackEnd back_end = new LSFBackEnd( config.getLsfQueue(),
                                              config.getLsfUser(),
                                              config.getLSFMemoryLimit(),
                                              config.getLSFMemoryReservationTimeout(),
                                              config.getLSFCPUCores() );
        back_end.setOutputFolderPath( Paths.get( config.getLsfOutputPath() ) );
        return back_end;
    }

    public void
    execute( StageInstance instance )
    {
        if( EvalResult.StageTransient == can_execute( instance ) )
        {
            log.info( String.format( "%sxecuting stage %s", instance.getExecutionCount() > 0 ? "E" : "Re-e", instance.getStageName() ) );

            List<String> p_args = constructArgs( instance, do_commit );

            LSFBackEnd back_end = configureBackend();

            ExternalCall ec     = back_end.new_call_instance( String.format( "%s--%s--%s",
                                                                             instance.getPipelineName(), 
                                                                             instance.getProcessID(),
                                                                             instance.getStageName() ), 
                                                              "java", 
                                                              p_args.toArray( new String[ p_args.size() ] ) );
            
            if( ec instanceof LSFClusterCall)
            {
            	LSFClusterCall call = ( (LSFClusterCall) ec );
            	call.setTaskLostExitCode( default_failure_result.getExitCode() );
            }

            log.info( ec.getCommandLine() );
            
            ec.execute();
            fillExecutionInfo( ec );
            
// Critical section to avoid constructing large strings multiple times            
            synchronized( LSFStageExecutor.class )
			{
            	String print_msg = String.format( "Finished execution of stage %s\n%s", 
                                                  instance.getStageName(),
                                                  new ExternalCallException( ec ).toString() );
            
                log.info( print_msg );
            }
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
        info.setThrowable( null );
        info.setLogMessage( new ExternalCallException( ec ).toString() );
    }


    public ExecutionInfo
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
        return LSFExecutorConfig.class;
    }


    @Override public <T extends ExecutorConfig> void
    configure( T params )
    {
        configure( (LSFExecutorConfig) params );
    }
}
