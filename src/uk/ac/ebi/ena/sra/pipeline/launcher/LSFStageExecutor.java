package uk.ac.ebi.ena.sra.pipeline.launcher;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

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
    private boolean  was_error;
    ExecutionInfo    info;
    final private    LSFBackEnd back_end;
    private String   config_prefix_name;
    private String   config_source_name;
	private String[] properties_pass;
	private ExecutionResult default_failure_result;

    
    public
    LSFStageExecutor( String pipeline_name, 
                      ResultTranslator translator )
    {
        this( pipeline_name, translator,
              DefaultConfiguration.CURRENT.getDefaultLSFQueue(),
              DefaultConfiguration.CURRENT.getDefaultLSFUser(),
              DefaultConfiguration.CURRENT.getDefaultLSFMem(),
              DefaultConfiguration.CURRENT.getDefaultLSFMemTimeout(),
              DefaultConfiguration.CURRENT.getDefaultLSFCpuCores(),
              Paths.get( DefaultConfiguration.CURRENT.getDefaultLSFOutputRedirection() ), 
              DefaultConfiguration.CURRENT.getConfigPrefixName(), 
              DefaultConfiguration.CURRENT.getConfigSourceName(),
              DefaultConfiguration.CURRENT.getPropertiesPass() );
    }
    
    
    public
    LSFStageExecutor( String pipeline_name, 
                      ResultTranslator translator, 
                      String queue,
                      String lsf_user,
                      int lsf_mem, 
                      int lsf_mem_timeout,
                      int lsf_cpu_cores )
    {
    	this( pipeline_name, translator, queue, lsf_user, lsf_mem, lsf_mem_timeout, lsf_cpu_cores, 
    	      Paths.get( DefaultConfiguration.CURRENT.getDefaultLSFOutputRedirection() ),
              DefaultConfiguration.CURRENT.getConfigPrefixName(), 
              DefaultConfiguration.CURRENT.getConfigSourceName(),
              DefaultConfiguration.CURRENT.getPropertiesPass() );
    }
    
    
    LSFStageExecutor( String pipeline_name, 
                      ResultTranslator translator, 
                      String queue,
                      String lsf_user,
                      int lsf_mem, 
                      int lsf_mem_timeout,
                      int lsf_cpu_cores,
                      Path output_path,
                      String config_prefix_name,
                      String config_source_name,
                      String[] properties_pass )
    {
        super( pipeline_name, translator );
        LSFBackEnd be = new LSFBackEnd( queue, lsf_user, lsf_mem, lsf_mem_timeout, lsf_cpu_cores );
        be.setOutputFolderPath( output_path );
        this.default_failure_result = translator.getCommitStatusDefaultFailure();
        this.back_end = be;
        this.config_prefix_name = config_prefix_name;
        this.config_source_name = config_source_name;
        this.properties_pass    = properties_pass;
    }
    
    
    public int 
    getLSFMemoryReservationTimeout() 
    { 
        return back_end.memory_reservation_timeout; 
    }
    
    
    public int 
    getLSFMemoryLimit() 
    { 
        return back_end.memory_limit; 
    }
    
    
    public int 
    getLSFCPUCores() 
    { 
        return back_end.cpu_cores;
    };
    
    
    //TODO: re-factor
    public int 
    getJavaMemoryLimit() 
    { 
        return getLSFMemoryLimit() - 1500; 
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
        {
            ( (LSFBackEnd)back_end ).cpu_cores = params.getLSFCPUCores();
            ( (LSFBackEnd)back_end ).memory_limit = params.getLSFMemoryLimit();
            ( (LSFBackEnd)back_end ).memory_reservation_timeout = params.getLSFMemoryReservationTimeout();
        }
    }
    
    
    public String[]
    getPropertiesPass()
    {
    	 return properties_pass; 
    }

    
    private List<String> 
    constructArgs( StageInstance instance, boolean commit )
    {
        List<String>p_args = new ArrayList<String>();


        p_args.add( "-XX:+UseSerialGC" );
     
        int memory_limit = getJavaMemoryLimit();
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
        
        p_args.add( uk.ac.ebi.ena.sra.pipeline.launcher.StageLauncher.PARAMETERS_NAME_ENABLED );
        p_args.add( Boolean.toString( instance.isEnabled() ).toLowerCase() );
        
        p_args.add( uk.ac.ebi.ena.sra.pipeline.launcher.StageLauncher.PARAMETERS_NAME_EXEC_COUNT );
        p_args.add( Long.toString( instance.getExecutionCount() ) );
        
        return p_args;
    }


	public void 
    execute( StageInstance instance )
    {
        if( EvalResult.StageTransient == can_execute( instance ) )
        {
            log.info( String.format( "%sxecuting stage %s", instance.getExecutionCount() > 0 ? "E" : "Re-e", instance.getStageName() ) );

            List<String> p_args = constructArgs( instance, do_commit );

            ExternalCall ec     = back_end.new_call_instance( String.format( "%s--%s--%s",
                                                                             instance.getPipelineName(), 
                                                                             instance.getProcessID(),
                                                                             instance.getStageName() ), 
                                                              "java", 
                                                              p_args.toArray( new String[ p_args.size() ] ) );

            if( ec instanceof LSFClusterCall  )
            {
            	LSFClusterCall call = ( (LSFClusterCall) ec );
            	call.setTaskLostExitCode( default_failure_result.getExitCode() );
                LSFExecutorConfig si_config = instance.getResourceConfig( LSFExecutorConfig.class );
                if( null != si_config )
                {
                	call.setMemoryLimit( si_config.getLSFMemoryLimit() );
                	call.setMemoryReservationTimeout( si_config.getLSFMemoryReservationTimeout() );
                	call.setCPUNumber( si_config.getLSFCPUCores() );
                }
            }
            
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
        info.setThrowable( null );
        info.setLogMessage( new ExternalCallException( ec ).toString() );
    }

    
    
    public ExecutionInfo
    get_info()
    {
        return info;
    }
    
    
    public boolean 
    was_error()
    {
        return was_error;
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
