package uk.ac.ebi.ena.sra.pipeline.dblock;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.ac.ebi.ena.sra.pipeline.configuration.OracleHeartBeatConnection;
import uk.ac.ebi.ena.sra.pipeline.launcher.LauncherLockManager;

public class 
DBLockManagerTest 
{
	static Logger log = Logger.getLogger( DBLockManagerTest.class );
	String LOCK_FILE_NAME = "lock.file";
	static Connection connection = null;
			
	@BeforeClass public static void
    setup() throws Throwable
    {
        PropertyConfigurator.configure( "resource/test.log4j.properties" );
        connection = createConnection();
    }
	
	
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

	
	
	@Test public void
	test() throws Exception
	{
		try( LauncherLockManager flman = new DBLockManager( connection, "TEST" ) )
		{
			flman.purge( Timestamp.from( Instant.now() ) );
			Assert.assertTrue( flman.tryLock( LOCK_FILE_NAME ) );
			Assert.assertFalse( flman.tryLock( LOCK_FILE_NAME ) );
			Assert.assertTrue( flman.unlock( LOCK_FILE_NAME ) );
			Assert.assertFalse( flman.unlock( LOCK_FILE_NAME ) );
		}		
		
		//Assert.assertTrue( FileLockManager.tryLock( LOCK_FILE_NAME ) );
		
	}

	
	@Test public void 
	testR()
	{
	    System.out.println( Math.floor( -2.5 ) );
	}

}
