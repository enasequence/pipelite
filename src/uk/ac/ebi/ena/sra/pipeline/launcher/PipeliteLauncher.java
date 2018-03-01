package uk.ac.ebi.ena.sra.pipeline.launcher;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import uk.ac.ebi.ena.sra.pipeline.resource.ResourceLocker;
import uk.ac.ebi.ena.sra.pipeline.storage.StorageBackend;


public class 
PipeliteLauncher
{
    //Contract for TaskIdSource: user is responsible for checking whether task was completed by pipeline or not  
    public interface TaskIdSource { public List<String> getTaskQueue() throws SQLException; }
    
    public interface ProcessFactory { public PipeliteProcess getProcess( String process_id ); }
    
    public interface 
    PipeliteProcess extends Runnable 
    { 
        public String                 getProcessId();
        public StageExecutor          getExecutor();
        default public void           setProcessID( String process_id ) { throw new RuntimeException( "Method must be overriden" ); }
        default public StorageBackend getStorage() { throw new RuntimeException( "Method must be overriden" ); }
        default public void           setStorage( StorageBackend storage ) { throw new RuntimeException( "Method must be overriden" ); }
        default public ResourceLocker getLocker() { throw new RuntimeException( "Method must be overriden" ); }
        default public void           setLocker( ResourceLocker locker ) { throw new RuntimeException( "Method must be overriden" ); }
        default public void           setExecutor( StageExecutor executor ) {}
        default public void           stop() {}
        default public boolean        isStopped() { return false; }
    }
    
    public interface StageExecutorFactory { public StageExecutor getExecutor(); }


    final static Map<Object, ProcessLauncher> tasks = Collections.synchronizedMap( new WeakHashMap<Object, ProcessLauncher>() ); 
    TaggedPoolExecutor thread_pool;
    Map<Future<?>, ProcessLauncher> task_map = new HashMap<Future<?>, ProcessLauncher>();
    
    final static int MEMORY_LIMIT = 15000; 
    TaskIdSource task_id_source;
    private ProcessFactory process_factory;
    private int source_read_timeout = 60 * 1000;
    private boolean exit_when_empty;
    private StageExecutorFactory executor_factory;
    private volatile boolean do_stop;
    private Logger   log = Logger.getLogger( this.getClass() );
    
    
    public void
    setProcessFactory( ProcessFactory process_factory )
    {
        this.process_factory = process_factory;
        
    }


    public void
    setTaskIdSource( TaskIdSource task_id_source )
    {
        this.task_id_source = task_id_source;
        
    }


    public TaskIdSource
    getTaskIdSource()
    {
        return this.task_id_source;
        
    }
    
    
    public void
    setProcessPool( ProcessPoolExecutor thread_pool )
    {
        this.thread_pool = thread_pool;
    }
    

    void
    shutdown()
    {
        thread_pool.shutdown();
        thread_pool.running.forEach( ( p, r ) -> {
            log.info( "Sending stop to " + p );
            ( (ProcessLauncher)r).stop(); 
        } );
        try
        {
            while( !thread_pool.awaitTermination( 30, TimeUnit.SECONDS ) )
            {
                log.info( "Awaiting for completion of " + thread_pool.getActiveCount() + " threads " );
            }
        } catch( InterruptedException ie )
        {
            Thread.currentThread().interrupt();
        }
    }
    
    
    public void
    stop()
    {
        this.do_stop = true; 
    }
    
    
    public boolean
    isStopped()
    {
        return this.do_stop; 
    }
    
    
    public void
    execute() throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException
    {
        List<String> task_queue = null;
main:   while( !do_stop && null != ( task_queue = ( thread_pool.getCorePoolSize() - thread_pool.getActiveCount() ) > 0 ? getTaskIdSource().getTaskQueue() : Collections.emptyList() ) )
        {
            if( exit_when_empty && task_queue.isEmpty() )
                break;
            
            for( String process_id : task_queue )
            {
                PipeliteProcess process = getProcessFactory().getProcess( process_id );
                process.setExecutor( getExecutorFactory().getExecutor() );
                try
                {
                    thread_pool.execute( process );
                } catch( RejectedExecutionException ree )
                {
                    break;
                }
            }           
            
            long until = System.currentTimeMillis() + getSourceReadTimout();
            while( until > System.currentTimeMillis() )
            {
                try
                {
                    Thread.sleep( 1000 );
                    if( 0 == thread_pool.getActiveCount() && !task_queue.isEmpty() )
                        break;
                } catch( InterruptedException e )
                {
                    Thread.currentThread().interrupt();
                    break main;
                }
            }
        }
    }
    
    
    public int
    getSourceReadTimout()
    {
        return source_read_timeout; 
    }
    
    
    public void
    setSourceReadTimeout( int source_read_timeout_ms )
    {
        this.source_read_timeout = source_read_timeout_ms;
    }

    
    ProcessFactory
    getProcessFactory()
    {
        return process_factory;
    }


    public boolean
    getExitWhenNoTasks()
    {
        return exit_when_empty;
    }


    public void
    setExitWhenNoTasks( boolean exit_when_empty )
    {
        this.exit_when_empty = exit_when_empty;
    }


    public void
    setExecutorFactory( StageExecutorFactory executor_factory )
    {
        this.executor_factory = executor_factory;
    }


    public StageExecutorFactory
    getExecutorFactory()
    {
        return this.executor_factory;
    }
}
