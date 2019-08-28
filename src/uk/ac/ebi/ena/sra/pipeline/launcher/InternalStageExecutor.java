package uk.ac.ebi.ena.sra.pipeline.launcher;

import uk.ac.ebi.ena.sra.pipeline.configuration.DefaultConfiguration;
import uk.ac.ebi.ena.sra.pipeline.executors.ExecutorConfig;
import uk.ac.ebi.ena.sra.pipeline.launcher.iface.ExecutionResult;
import uk.ac.ebi.ena.sra.pipeline.launcher.iface.StageTask;


public class 
InternalStageExecutor extends AbstractStageExecutor
{
    private ExecutionInfo info;
    private boolean do_commit;
    protected ExternalCallBackEnd back_end = new SimpleBackEnd(); 
    private StageTask task = null;
    
    
    public
    InternalStageExecutor( ResultTranslator translator )
    {
        super( "", translator );
    }
    
    
    @Override public void
    reset( StageInstance instance )
    {
        instance.setExecutionInstance( new ExecutionInstance() );
    }
    
    
    public void 
    execute( StageInstance instance ) 
    {
        ExecutionResult execution_result = null;
        Throwable       exception        = null;
        

        if( EvalResult.StageTransient == can_execute( instance ) )
        {
            try
            {
                if( null != DefaultConfiguration.currentSet().getPropertyPrefixName() )
                {
                    System.setProperty( DefaultConfiguration.currentSet().getPropertyPrefixName(), 
                                        DefaultConfiguration.currentSet().getPropertySourceName() );
                }
                
                Class<? extends StageTask> klass = DefaultConfiguration.currentSet().getStage( instance.getStageName() ).getTaskClass();
                task = (StageTask) klass.getConstructor( (Class[]) null ).newInstance( (Object[]) null );
                task.init( instance.getProcessID(), do_commit );
                task.execute();
                
            } catch( Throwable e )
            {
                e.printStackTrace();
                exception = e;
            } finally
            {
                info = new ExecutionInfo();
                info.setThrowable( exception );
                info.setExitCode( Integer.valueOf( TRANSLATOR.getCommitStatus( exception ).getExitCode() ) );

                if( null != task )
                    task.unwind();
            }
        }
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
        return do_commit;
    }

    @Override
    public Class<? extends ExecutorConfig> getConfigClass() {
        return null;
    }


    public StageTask 
    get_task()
    {
        return task;
    }    
    
    
    @Override public void
    configure( ExecutorConfig rc )
    {
        //empty
    }
}
