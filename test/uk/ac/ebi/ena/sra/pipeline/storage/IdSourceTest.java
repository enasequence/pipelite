package uk.ac.ebi.ena.sra.pipeline.storage;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;






import org.apache.commons.dbutils.DbUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;






import uk.ac.ebi.ena.sra.pipeline.configuration.OracleHeartBeatConnection;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncher;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteState;
import uk.ac.ebi.ena.sra.pipeline.launcher.StageInstance;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncher.TaskIdSource;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteState.State;
import uk.ac.ebi.ena.sra.pipeline.mock.schedule.ExecutionResults;
import uk.ac.ebi.ena.sra.pipeline.storage.OracleProcessIdSource;
import uk.ac.ebi.ena.sra.pipeline.storage.OracleStorage;
import uk.ac.ebi.ena.sra.pipeline.storage.StorageBackend;
import uk.ac.ebi.ena.sra.pipeline.storage.StorageBackend.StorageException;



public class 
IdSourceTest
{
    static TaskIdSource   id_src;
    static StorageBackend db_backend;
    static Logger log = Logger.getLogger( IdSourceTest.class );
    final static String PIPELINE_NAME = "TEST_PIPELINE";
    static Connection connection;
    
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
    setup() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException
    {
        PropertyConfigurator.configure( "resource/test.log4j.properties" );
    
        connection = createConnection();
        
        OracleProcessIdSource ps = new OracleProcessIdSource();
        ps.setTableName( "PIPELITE_PROCESS" );
        ps.setExecutionResultArray( ExecutionResults.values() );
        ps.setRedoCount( Integer.MAX_VALUE );
        ps.setConnection( connection );
        ps.setPipelineName( PIPELINE_NAME );
        
        ps.init();
        id_src = ps;
                
        OracleStorage os = new OracleStorage();
        os.setPipelineName( PIPELINE_NAME );
        os.setProcessTableName( "PIPELITE_PROCESS" );
        os.setStageTableName( "PIPELITE_STAGE" );
        os.setConnection( connection );
        db_backend = os;
    }
    
    
    @AfterClass public static void
    whack()
    {
        DbUtils.rollbackAndCloseQuietly( connection );
    }
    
    
    
    @Test public void
    main() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, InterruptedException
    {                     
        List<String> ids = Stream.of( "PROCESS_ID1", "PROCESS_ID2" ).collect( Collectors.toList() );
        ids.stream().forEach( i -> {
                StageInstance si = new StageInstance();
                si.setPipelineName( PIPELINE_NAME );
                si.setStageName( "STAGE" );
                si.setProcessID( i );
                try
                {
                    db_backend.save( si );
                } catch( Exception e )
                {
                    throw new RuntimeException( e );
                }
        } );

        List<PipeliteState> saved = saveTasks( PIPELINE_NAME, ids );

        AtomicInteger cnt = new AtomicInteger( ids.size() );
        
        saved.stream().forEach( e -> e.setPriority( 0 ) );
        saved.stream().forEach( e -> { 
            try
            {
                db_backend.save( e );
            } catch( Exception e1 )
            {
                throw new RuntimeException( e1 ); 
            } 
        } );
        

        List<String> stored = id_src.getTaskQueue();
        ids.stream().forEach( i -> {
            stored.stream().filter( e -> e.equals( i ) ).findFirst().ifPresent( e -> cnt.decrementAndGet() ); 
        } );
        
        Assert.assertEquals( 0, cnt.get() );

        AtomicInteger priority = new AtomicInteger();
        saved.stream().forEach( e -> e.setPriority( priority.getAndAdd( 4 ) ) );
        saved.stream().forEach( e -> { 
            try
            {
                db_backend.save( e );
            } catch( Exception e1 )
            {
                throw new RuntimeException( e1 ); 
            } 
        } );
        
        
        Assert.assertEquals( stored.get( stored.size() - 1 ), id_src.getTaskQueue().get( 0 ) );
    }
    
    
    private List<PipeliteState>
    saveTasks( String pipeline_name, List<String> ids )
    {
        return ids.stream().map( id -> 
        { 
            PipeliteState result; 
            try
            {
                result = new PipeliteState( pipeline_name, id );
                result.setProcessComment( "PROCESS_COMMENT" );
                result.setState( State.ACTIVE );
                db_backend.save( result );
                
                log.info( result );
                
            } catch( Exception e )
            {
                throw new RuntimeException( e );
            } return result; 
        } ).collect( Collectors.toList() );
    }
}