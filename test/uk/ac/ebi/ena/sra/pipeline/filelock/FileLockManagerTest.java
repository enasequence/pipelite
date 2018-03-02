package uk.ac.ebi.ena.sra.pipeline.filelock;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class 
FileLockManagerTest 
{
	static Logger log = Logger.getLogger( FileLockManagerTest.class );
	String LOCK_FILE_NAME = "lock.file";
    
	@BeforeClass public static void
    setup()
    {
        PropertyConfigurator.configure( "resource/test.log4j.properties" );
    }
	
	
	@Test public void
	test() throws Exception
	{
		try( FileLockManager flman = new FileLockManager() )
		{
			log.info( "Port: " + flman.getPort() );
			Files.write( Paths.get( LOCK_FILE_NAME ), "123@localhost:9999 99".getBytes(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING );
			Assert.assertTrue( flman.tryLock( LOCK_FILE_NAME ) );
			Assert.assertFalse( flman.tryLock( LOCK_FILE_NAME ) );
		}		
		
		//Assert.assertTrue( FileLockManager.tryLock( LOCK_FILE_NAME ) );
		
	}


}
