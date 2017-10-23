package uk.ac.ebi.ena.sra.pipeline.storage;

import static org.mockito.Mockito.mock;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
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
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncher.TaskIdSource;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteState;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteState.State;
import uk.ac.ebi.ena.sra.pipeline.launcher.StageInstance;
import uk.ac.ebi.ena.sra.pipeline.launcher.iface.Stage;
import uk.ac.ebi.ena.sra.pipeline.mock.schedule.ExecutionResults;



public class 
OracleStorageTest
{
    static TaskIdSource   id_src;
    static StorageBackend db_backend;
    static Logger log = Logger.getLogger( OracleStorageTest.class );
    final static String PIPELINE_NAME = "RUN_PROCESS";
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
        //DbUtils.commitAndCloseQuietly( connection );
        DbUtils.rollbackAndCloseQuietly( connection );
    }
    
    
    @Test public void
    main() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, InterruptedException
    {                     
        List<String> ids = Stream.of( "PROCESS_ID1", "PROCESS_ID2" ).collect( Collectors.toList() );
        AtomicInteger cnt1 = new AtomicInteger( ids.size() );
        
        ids.stream().forEach( i -> {
            try
            {
                loadTasks( PIPELINE_NAME, Arrays.asList( new String[] { i } ) );
            } catch( RuntimeException e )
            {
                cnt1.decrementAndGet();
            }
        } );
        Assert.assertEquals( 0, cnt1.get() );
        
        List<PipeliteState> saved = saveTasks( PIPELINE_NAME, ids );
        List<PipeliteState> loaded = loadTasks( PIPELINE_NAME, ids );
        Assert.assertArrayEquals( saved.toArray( new PipeliteState[ saved.size() ] ), loaded.toArray( new PipeliteState[ loaded.size() ] ) );

        
        
        
        Stage[] stages = new Stage[] { mock( Stage.class ), mock( Stage.class ), mock( Stage.class ), mock( Stage.class ) };
        
        // Try to load not existing stages;
        AtomicInteger cnt = new AtomicInteger( stages.length );
        Stream.of( stages ).forEach( s -> { try{ 
                                                 loadStages( PIPELINE_NAME, ids.get( 0 ), s ); 
                                            } catch( RuntimeException e )
                                            {
                                                cnt.decrementAndGet();
                                            }
        } );
        Assert.assertEquals( 0, cnt.get() );
        
        List<StageInstance> si = saveStages( PIPELINE_NAME, ids.get( 0 ), stages );
        List<StageInstance> li = loadStages( PIPELINE_NAME, ids.get( 0 ), stages );
        Assert.assertArrayEquals( si.toArray(), li.toArray() );
        
        List<StageInstance> ui = li.stream().map( e -> { StageInstance r = new StageInstance( e );
                                                         r.setExecutionCount( r.getExecutionCount() + 1 );
                                                         return r; } ).collect( Collectors.toList() );
        Assert.assertNotEquals( li, ui );
        
        List<StageInstance> sui = saveStages( ui );
        List<StageInstance> lui = loadStages( PIPELINE_NAME, ids.get( 0 ), stages );
        Assert.assertEquals( sui, lui );
    }


    private List<StageInstance>
    saveStages( List<StageInstance> stages )
    {
        stages.stream().forEach( s -> { 
            try
            {
                db_backend.save( s );
            } catch( Exception e )
            {
                throw new RuntimeException( e );
            } 
        } );
        return stages;
    }
    
    
    private List<StageInstance>
    saveStages( String pipeline_name, String process_id, Stage...stages )
    {
        List<StageInstance> result = new ArrayList<StageInstance>();
        
        Stream.of( stages ).forEach( s -> { 
            StageInstance si = new StageInstance();
            si.setPipelineName( pipeline_name );
            si.setEnabled( true );
            si.setProcessID( process_id );
            si.setStageName( s.toString() );
            result.add( si );
            try
            {
                db_backend.save( si );
            } catch( Exception e )
            {
                throw new RuntimeException( e );
            } 
        } );
        
        
        return result;
    }
    
    
    private List<StageInstance>
    loadStages( String pipeline_name, String process_id, Stage...stages )
    {
        List<StageInstance> result = new ArrayList<StageInstance>();

        Stream.of( stages ).forEach( s -> { 
            StageInstance si = new StageInstance();
            si.setPipelineName( pipeline_name );
            si.setEnabled( true );
            si.setProcessID( process_id );
            si.setStageName( s.toString() );
            result.add( si );
            
            try
            {
                db_backend.load( si );
            } catch( Exception e )
            {
                throw new RuntimeException( e );
            } 
        } );
        
        
        return result;
    }
    
    
    private List<PipeliteState>
    loadTasks( String pipeline_name, List<String> ids )
    {
        return ids.stream().map( id -> 
        { 
            PipeliteState result; 
            try
            {
                db_backend.load( result = new PipeliteState( pipeline_name, id ) );
            } catch( Exception e )
            {
                throw new RuntimeException( e );
            } return result; 
        } ).collect( Collectors.toList() );
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
            } catch( Exception e )
            {
                throw new RuntimeException( e );
            } return result; 
        } ).collect( Collectors.toList() );
    }
}