package uk.ac.ebi.ena.sra.pipeline.launcher;

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
    public static final int LSF_JVM_MEMORY_DELTA_MB = 1500;

    private boolean  do_commit = true;
    ExecutionInfo    info;
    private String   config_prefix_name;
    private String   config_source_name;
    private ExecutionResult default_failure_result;
    private String[] properties_pass;
    private LSFExecutorConfig config;
    private int lsf_memory_limit;
    private int cpu_cores;


    public
    LSFStageExecutor( String pipeline_name, 
                      ResultTranslator translator,
                      int lsf_memory_limit,
                      int cpu_cores,
                      LSFExecutorConfig config )
    {
    	this( pipeline_name, translator,
              lsf_memory_limit,
              cpu_cores,
              DefaultConfiguration.CURRENT.getConfigPrefixName(),
              DefaultConfiguration.CURRENT.getConfigSourceName(),
              DefaultConfiguration.CURRENT.getPropertiesPass(),
              config );
    }
    
    
    LSFStageExecutor( String pipeline_name, 
                      ResultTranslator translator,
                      int lsf_memory_limit,
                      int cpu_cores,
                      String config_prefix_name,
                      String config_source_name,
                      String[] properties_pass,
                      LSFExecutorConfig config )
    {
        super( pipeline_name, translator );
        this.default_failure_result = translator.getCommitStatusDefaultFailure();
        this.config_prefix_name = config_prefix_name;
        this.config_source_name = config_source_name;

        this.lsf_memory_limit = lsf_memory_limit;
        this.cpu_cores = cpu_cores;
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
     
        int lsf_memory_limit = instance.getMemoryLimit();
        if( lsf_memory_limit <= 0 ) {
            lsf_memory_limit = this.lsf_memory_limit;
        }

        int java_memory_limit = lsf_memory_limit - LSF_JVM_MEMORY_DELTA_MB;

        if( 0 >= java_memory_limit )
        {
            log.warn( "LSF memory is lower than " + LSF_JVM_MEMORY_DELTA_MB + "MB. Java memory limit will not be set." );
        } else
        {
            p_args.add( String.format( "-Xmx%dM", java_memory_limit ) );
        }

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
    configureBackend( StageInstance instance )
    {
        int mem = instance.getMemoryLimit();
        if( mem <= 0 ) {
            mem = lsf_memory_limit;
        }
        int cpu = instance.getCPUCores();
        if( cpu <= 0 ) {
            cpu = cpu_cores;
        }

        LSFBackEnd back_end = new LSFBackEnd( config.getLsfQueue(),
                                              mem,
                                              config.getLSFMemoryReservationTimeout(),
                                              cpu );
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

            LSFBackEnd back_end = configureBackend( instance );

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
