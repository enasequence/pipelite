package uk.ac.ebi.ena.sra.pipeline.launcher;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import uk.ac.ebi.ena.sra.pipeline.base.external.ExternalCall;
import uk.ac.ebi.ena.sra.pipeline.base.external.ExternalCallException;
import uk.ac.ebi.ena.sra.pipeline.base.external.LSFClusterCall;
import uk.ac.ebi.ena.sra.pipeline.base.external.lsf.LSFJobDescriptor;
import uk.ac.ebi.ena.sra.pipeline.configuration.DefaultConfiguration;
import uk.ac.ebi.ena.sra.pipeline.executors.ExecutorConfig;
import uk.ac.ebi.ena.sra.pipeline.executors.LSFExecutorConfig;


public class 
LSFStageExecutor extends AbstractStageExecutor implements LSFExecutorConfig
{
    private boolean  do_commit = true;
    private boolean  was_error;
    ExecutionInfo    info;
    final private    ExternalCallBackEnd back_end; 
    private          ExecutorConfig rc = new LSFExecutorConfig() {};

    
    public
    LSFStageExecutor( String pipeline_name, 
                      ResultTranslator translator )
    {
        this( pipeline_name, translator,
              DefaultConfiguration.CURRENT.getDefaultLSFQueue(),
              DefaultConfiguration.CURRENT.getDefaultLSFUser(),
              DefaultConfiguration.CURRENT.getDefaultLSFMem(),
              DefaultConfiguration.CURRENT.getDefaultLSFMemTimeout(),
              DefaultConfiguration.CURRENT.getDefaultLSFCpuCores() );
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
        super( pipeline_name, translator );
        LSFBackEnd be = new LSFBackEnd( queue, lsf_user, lsf_mem, lsf_mem_timeout, lsf_cpu_cores );
        be.setOutputFolderPath( Paths.get( DefaultConfiguration.CURRENT.getDefaultLSFOutputRedirection() ) );
        this.back_end = (ExternalCallBackEnd) be;
    }
    
    
    @Override public int 
    getLSFMemoryReservationTimeout() 
    { 
        return null == rc ? LSFExecutorConfig.super.getLSFMemoryReservationTimeout() 
        		          : ((LSFExecutorConfig)rc).getLSFMemoryReservationTimeout(); 
    }
    
    
    @Override public int 
    getLSFMemoryLimit() 
    { 
        return null == rc ? LSFExecutorConfig.super.getLSFMemoryLimit() 
        		          : ((LSFExecutorConfig)rc).getLSFMemoryLimit(); 
    }
    
    
    @Override public int 
    getLSFCPUCores() 
    { 
        return null == rc ? LSFExecutorConfig.super.getLSFCPUCores() 
        		          : ((LSFExecutorConfig)rc).getLSFCPUCores(); 
    };
    
    
    @Override public int 
    getJavaMemoryLimit() 
    { 
        return null == rc ? LSFExecutorConfig.super.getJavaMemoryLimit() 
        		          : ((LSFExecutorConfig)rc).getJavaMemoryLimit(); 
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
            this.rc = params;
        }
    }
    
    //TODO looks overengineered
    @Override public String[]
    getPropertiesPass()
    {
    	 return null == rc ? LSFExecutorConfig.super.getPropertiesPass() 
    			           : ((LSFExecutorConfig)rc).getPropertiesPass(); 
    }

    
    private List<String> 
    constructArgs( StageInstance instance, boolean commit )
    {
        List<String>p_args = new ArrayList<String>();


        p_args.add( "-XX:+UseSerialGC" );
     
        int memory_limit = getJavaMemoryLimit();
        if( 0 < memory_limit ) // TODO check
            p_args.add( String.format( "-Xmx%dM", memory_limit ) );
        
        p_args.add( String.format( "-D%s=%s", 
                                   DefaultConfiguration.currentSet().getConfigPrefixName(), 
                                   DefaultConfiguration.currentSet().getConfigSourceName() ) ); 

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
                                                                             PIPELINE_NAME, //TODO: get from instance 
                                                                             instance.getProcessID(),
                                                                             instance.getStageName() ), 
                                                              "java", 
                                                              p_args.toArray( new String[ p_args.size() ] ) );

            if( ec instanceof LSFClusterCall  )
            {
                ( (LSFClusterCall) ec ).setMemoryLimit( getLSFMemoryLimit() );
                ( (LSFClusterCall) ec ).setMemoryReservationTimeout( getLSFMemoryReservationTimeout() );
                ( (LSFClusterCall) ec ).setCPUNumber( getLSFCPUCores() );
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
    

    @Override public void
    configure( ExecutorConfig rc )
    {
        this.rc = rc;
    }
}
