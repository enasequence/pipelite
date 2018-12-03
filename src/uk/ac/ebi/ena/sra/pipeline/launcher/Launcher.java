package uk.ac.ebi.ena.sra.pipeline.launcher;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.beust.jcommander.JCommander;

import uk.ac.ebi.ena.sra.pipeline.base.external.LSFClusterCall.LSFQueue;
import uk.ac.ebi.ena.sra.pipeline.configuration.DefaultConfiguration;
import uk.ac.ebi.ena.sra.pipeline.configuration.DefaultExecutorFactory;
import uk.ac.ebi.ena.sra.pipeline.configuration.DefaultLauncherParams;
import uk.ac.ebi.ena.sra.pipeline.configuration.DefaultProcessFactory;
import uk.ac.ebi.ena.sra.pipeline.dblock.DBLockManager;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncher.PipeliteProcess;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncher.TaskIdSource;
import uk.ac.ebi.ena.sra.pipeline.resource.ResourceLocker;
import uk.ac.ebi.ena.sra.pipeline.storage.OracleProcessIdSource;
import uk.ac.ebi.ena.sra.pipeline.storage.OracleStorage;
import uk.ac.ebi.ena.sra.pipeline.storage.StorageBackend;
import uk.ac.ebi.ena.sra.pipeline.storage.StorageBackend.StorageException;


public class 
Launcher
{
    final static Map<Object, ProcessLauncher> tasks = Collections.synchronizedMap( new WeakHashMap<Object, ProcessLauncher>() ); 
    TaggedPoolExecutor thread_pool;
    Map<Future<?>, ProcessLauncher> task_map = new HashMap<Future<?>, ProcessLauncher>();
    
    final static int MEMORY_LIMIT = 15000;
    private static final int DEFAULT_ERROR_EXIT = 1;
    private static final int NORMAL_EXIT = 0; 
    
    
    private static ProcessPoolExecutor
    init( int workers, StorageBackend storage, ResourceLocker locker ) throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException
    {
         return new ProcessPoolExecutor( workers ) 
                    { 
                          public void 
                          unwind( PipeliteProcess process ) 
                          { 
                              StorageBackend storage = ((PipeliteProcess)process).getStorage();
                              try
                              {
                                  storage.flush();
                              } catch( StorageException e )
                              {
                                  // TODO Auto-generated catch block
                                  e.printStackTrace();
                              } 
                            
//                              try
//                              {
//                                  storage.close();
//                              } catch( StorageException e )
//                              {
//                                  // TODO Auto-generated catch block
//                                  e.printStackTrace();
//                              } 
                          }
                          
                          
                          public void 
                          init( PipeliteProcess process ) 
                          { 
                              ((PipeliteProcess)process).setStorage( storage );
                              ((PipeliteProcess)process).setLocker( locker );
                          }
                      };
    }
    
    
    private static OracleStorage
    initStorageBackend() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException
    {
        OracleStorage os = new OracleStorage();
        
        Connection connection = DefaultConfiguration.currentSet().createConnection();
        connection.setAutoCommit( false );
        
        os.setConnection( connection );
        os.setProcessTableName( DefaultConfiguration.currentSet().getProcessTableName() );
        os.setStageTableName( DefaultConfiguration.currentSet().getStageTableName() );
        os.setPipelineName( DefaultConfiguration.currentSet().getPipelineName() );
        os.setLogTableName( DefaultConfiguration.currentSet().getLogTableName() );
        return os;
    }
    
    
    
    private static TaskIdSource
    initTaskIdSource() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException
    {
        OracleProcessIdSource ts = new OracleProcessIdSource();
        
        ts.setConnection( DefaultConfiguration.currentSet().createConnection() );
        ts.setTableName( DefaultConfiguration.currentSet().getProcessTableName() );
        ts.setPipelineName( DefaultConfiguration.currentSet().getPipelineName() );
        ts.setRedoCount( DefaultConfiguration.currentSet().getStagesRedoCount() );
        ts.setExecutionResultArray( DefaultConfiguration.currentSet().getCommitStatus() );
        ts.init();
        
        return ts;
    }
    
    
    
    
    public static void 
    main( String[] args ) throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException 
    {
        DefaultLauncherParams params  = new DefaultLauncherParams();
        JCommander jc  = new JCommander( params );
        LSFQueue queue = DefaultLauncherParams.DEFAULT_LSF_QUEUE;

        try
        {
            jc.parse( args );
            queue = LSFQueue.findByName( params.queue_name );
            if( null == queue )
            {
                System.out.println( "Available queues: " );
                for( LSFQueue q : LSFQueue.values() )
                    System.out.println( q.getQueueName() );
            }
            
        }catch( Exception e )
        {
            System.out.println( "**" );
            jc.usage();
            System.exit( DEFAULT_ERROR_EXIT );
        }
        System.exit( main2( params ) );
    }


    private static int 
    main2( DefaultLauncherParams params ) throws IOException
    {
        PatternLayout   layout = new PatternLayout( "%d{ISO8601} %-5p [%t] " + DefaultConfiguration.currentSet().getPipelineName() + " %c{1}:%L - %m%n" );
        FileAppender  appender = new DailyRollingFileAppender( layout, params.log_file, "'.'yyyy-ww" );
        appender.setThreshold( Level.ALL );
        Logger.getRootLogger().removeAllAppenders();
        Logger.getRootLogger().addAppender( appender );
        Logger.getRootLogger().setLevel( Level.ALL );

        
        TaskIdSource task_id_source = null;        
        PipeliteLauncher launcher = new PipeliteLauncher();
        OracleStorage    storage = null;
        CountDownLatch latch = new CountDownLatch( 1 );

        try( Connection connection = DefaultConfiguration.currentSet().createConnection() )
        {
        	try( LauncherLockManager lockman = new DBLockManager( connection, DefaultConfiguration.currentSet().getPipelineName() ) )
        	{
	            storage  = initStorageBackend();
	            
	            if( lockman.tryLock( params.lock ) )
	            {
		            task_id_source = initTaskIdSource();
		            
		            
		            launcher.setTaskIdSource( task_id_source );
		            launcher.setProcessFactory( new DefaultProcessFactory() );
		            launcher.setExecutorFactory( new DefaultExecutorFactory( DefaultConfiguration.currentSet().getPipelineName(),
		                                                                     new ResultTranslator( DefaultConfiguration.currentSet().getCommitStatus() ), 
		                                                                     params.queue_name, 
		                                                                     params.lsf_user,
		                                                                     params.lsf_mem, 
		                                                                     params.lsf_mem_timeout,
		                                                                     params.lsf_cpu_cores,
		                                                                     DefaultConfiguration.currentSet().getStagesRedoCount() ) );
		            
		            launcher.setSourceReadTimeout( 120 * 1000 );
		            launcher.setProcessPool( init( params.workers, storage, (ResourceLocker)lockman ) );
	
		            //TODO remove
		            Runtime.getRuntime().addShutdownHook( new Thread( new Runnable() 
		            { 
		                Thread t = Thread.currentThread(); 
		                @Override public void 
		                run() 
		                { 
		                    launcher.stop();
	System.out.println( t.getName() + " Stop requested from " + Thread.currentThread().getName() );	                    
		                    try
	                        {
	                            latch.await();
	                            t.interrupt();
	System.out.println( t.getName() + " exited" );                            
	                        } catch( InterruptedException e )
	                        {
	                            // TODO Auto-generated catch block
	                            e.printStackTrace();
	                        }
		                } 
		            } ) );
		            
		            launcher.execute();
		            //TODO: check that all processes unlocks themselves
		            lockman.unlock( params.lock );
	
	            } else
	            {
	                System.out.println( String.format( "another instance of %s is already running %s", Launcher.class.getName(), Files.exists( Paths.get( params.lock ) ) ? Files.readAllLines( Paths.get( params.lock ) ) : params.lock ) );
	                return DEFAULT_ERROR_EXIT;
	            }
	            
	            return NORMAL_EXIT;
	        }catch( Throwable e )
	        {
	            e.printStackTrace();
	            return DEFAULT_ERROR_EXIT;
	            
	        }finally
	        {
	            try
	            {
	                launcher.shutdown();
	            } catch( Throwable t )
	            {
	                t.printStackTrace();
	            }
	            
	            try
	            {
	            	if( null != task_id_source && task_id_source instanceof OracleProcessIdSource )
	            		((OracleProcessIdSource)task_id_source).done();
	            } catch( Throwable t )
	            {
	                t.printStackTrace();
	            }
	            
	            try
	            {
	                storage.flush();
	            } catch( StorageException e )
	            {
	                e.printStackTrace();
	            } 
	
	            try
	            {
	                storage.close();
	            } catch( StorageException e )
	            {
	                e.printStackTrace();
	            } 
	            
	            latch.countDown();
	        }

        }catch( Throwable e )
        {
            e.printStackTrace();
            return DEFAULT_ERROR_EXIT;
        }
    }
}
