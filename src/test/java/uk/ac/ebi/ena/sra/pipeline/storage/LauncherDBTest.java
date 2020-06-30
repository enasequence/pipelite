package uk.ac.ebi.ena.sra.pipeline.storage;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.PropertyConfigurator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.ac.ebi.ena.sra.pipeline.configuration.OracleHeartBeatConnection;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncher;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncher.PipeliteProcess;
import uk.ac.ebi.ena.sra.pipeline.launcher.ProcessPoolExecutor;
import uk.ac.ebi.ena.sra.pipeline.launcher.StageExecutor;
import uk.ac.ebi.ena.sra.pipeline.mock.schedule.ExecutionResults;



public class 
LauncherDBTest
{
    final static long delay   = 5 * 1000;
    final static int  workers = ForkJoinPool.getCommonPoolParallelism();
    
    public static Connection
    createConnection() throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException
    {
        return createConnection( "era", "eradevt1", "jdbc:oracle:thin:@ (DESCRIPTION = (ADDRESS_LIST = (ADDRESS = (PROTOCOL = TCP)(HOST = ora-dlvm5-008.ebi.ac.uk)(PORT = 1521))) (CONNECT_DATA = (SERVICE_NAME = VERADEVT) (SERVER = DEDICATED)))" );
    }
    
    
    public static Connection
    createConnection( String user, String passwd, String url ) throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException
    {
        
        Properties props = new Properties();
        props.put( "user", user );
        props.put( "password", passwd );
        props.put( "SetBigStringTryClob", "true" );

        Class.forName( "oracle.jdbc.driver.OracleDriver" );
        Connection connection = new OracleHeartBeatConnection( DriverManager.getConnection( url, props ) );
        connection.setAutoCommit( false );

        return connection;
    }
    
    
    @BeforeClass public static void
    setup()
    {
        PropertyConfigurator.configure( "resource/test.log4j.properties" );
    }
    
    
    
    @Test public void
    main() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, InterruptedException
    {                                                     
        Connection connection = createConnection();

        OracleTaskIdSource id_src = new OracleTaskIdSource();
        id_src.setTableName( "PIPELITE_STAGE" );
        id_src.setExecutionResultArray( ExecutionResults.values() );
        id_src.setRedoCount( Integer.MAX_VALUE );
        id_src.setConnection( connection );
        id_src.init();
        
        PipeliteLauncher.ProcessFactory pr_src = new PipeliteLauncher.ProcessFactory() 
        {
            @Override public PipeliteProcess
            getProcess( String process_id )
            {
                return new PipeliteProcess()
                {
                    @Override public void
                    run()
                    {
                        System.out.println( "EXECUTING " + process_id );
                        try
                        {
                            Thread.sleep( delay );
                        } catch( InterruptedException e )
                        {
                            Thread.currentThread().interrupt();
                        }
//                        if( ThreadLocalRandom.current().nextDouble() > 0.5 )
//                            throw new RuntimeException();
//                        else 
                            throw new Error();
                    }

                    @Override public String
                    getProcessId()
                    {
                        return process_id;
                    }

                    @Override public StageExecutor
                    getExecutor()
                    {
                        return null;
                    }
                    
                };
            }
        };
        
        
        ProcessPoolExecutor pool = new ProcessPoolExecutor( workers ) 
        { 
              public void 
              unwind( PipeliteProcess process ) 
              { 
                  System.out.println( "FINISHED " + process.getProcessId() );
              }
              
              
              public void 
              init( PipeliteProcess process ) 
              { 
                  System.out.println( "INIT     " + process.getProcessId() );
              }
        };
        
        
        PipeliteLauncher l = new PipeliteLauncher();
        l.setTaskIdSource( id_src );
        l.setSourceReadTimeout( 1 );
        l.setProcessFactory( pr_src );
        l.setProcessPool( pool );
        l.setExitWhenNoTasks( true );
        
        long start = System.currentTimeMillis();        
        l.execute();

        pool.shutdown();
        pool.awaitTermination( 1, TimeUnit.MINUTES );

        long finish = System.currentTimeMillis();
        
System.out.println( "Completed: " + pool.getCompletedTaskCount() + " for " + ( finish - start ) + " mS using " + workers + " thread(s)" );
System.out.println( "CPU count: " + Runtime.getRuntime().availableProcessors() );
System.out.println( "Available parallelism: " + ForkJoinPool.getCommonPoolParallelism() );

        Assert.assertTrue( 0 == pool.getActiveCount() ); //Threads should properly react to interrupt
        pool.shutdownNow();
    }


    public void
        setProcessID( String process_id )
    {
    }


    public StorageBackend
        getStorage()
    {
        return null;
    }


    public void
        setStorage( StorageBackend storage )
    {
    }


    public void
        setExecutor( StageExecutor executor )
    {
    }
}