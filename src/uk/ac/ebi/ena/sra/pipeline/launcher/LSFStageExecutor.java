package uk.ac.ebi.ena.sra.pipeline.launcher;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import uk.ac.ebi.ena.sra.pipeline.base.external.ExternalCall;
import uk.ac.ebi.ena.sra.pipeline.base.external.ExternalCallException;
import uk.ac.ebi.ena.sra.pipeline.configuration.DefaultConfiguration;
import uk.ac.ebi.ena.sra.pipeline.executors.ExecutorConfig;
import uk.ac.ebi.ena.sra.pipeline.executors.LSFExecutorConfig;
import uk.ac.ebi.ena.sra.pipeline.launcher.iface.ExecutionResult;


public class 
LSFStageExecutor extends AbstractStageExecutor
{

    private String lsf_queue;
    private String lsf_user;
    private final Path lsf_output_path;
    private boolean  do_commit = true;
    ExecutionInfo    info;
    private String   config_prefix_name;
    private String   config_source_name;
    private String[] properties_pass;
    private ExecutionResult default_failure_result;
    private int cpu_cores;
    private int lsf_memory_limit;
    private int lsf_memory_reservation_timeout;
    private int java_memory_limit;


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
        this.config_prefix_name = config_prefix_name;
        this.config_source_name = config_source_name;

        this.cpu_cores = lsf_cpu_cores;
        this.lsf_memory_limit = lsf_mem;
        this.lsf_memory_reservation_timeout = lsf_mem_timeout;
        this.java_memory_limit = -1;
        this.properties_pass = properties_pass;
        this.lsf_queue = queue;
        this.lsf_user = lsf_user;
        this.lsf_output_path = output_path;
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
            cpu_cores = params.getLSFCPUCores();
            lsf_memory_limit = params.getLSFMemoryLimit();
            lsf_memory_reservation_timeout = params.getLSFMemoryReservationTimeout();
            java_memory_limit = params.getJavaMemoryLimit();
            properties_pass = params.getPropertiesPass();
            lsf_user = params.getLsfUser();
            lsf_queue = params.getLsfQueue();
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
     
        int memory_limit = java_memory_limit;

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


    private LSFBackEnd
    configureBackend()
    {
        LSFBackEnd back_end = new LSFBackEnd( lsf_queue, lsf_user, lsf_memory_limit, lsf_memory_reservation_timeout, cpu_cores );
        back_end.setOutputFolderPath( lsf_output_path );
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
