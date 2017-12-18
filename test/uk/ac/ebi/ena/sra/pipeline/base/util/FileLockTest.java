package uk.ac.ebi.ena.sra.pipeline.base.util;

import java.io.File;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import uk.ac.ebi.ena.sra.pipeline.base.util.FileLocker.FileLockException;

public class 
FileLockTest 
{
	static Logger log = Logger.getLogger( FileLockTest.class );
	final static String LOCK_FILE_NAME = "test_lock_file.lock";
    
	@BeforeClass public static void
    setup()
    {
        PropertyConfigurator.configure( "resource/test.log4j.properties" );
    }
	
	
	@Test public void 
	lockTest() throws FileLockException, Exception
	{
        try( FileLocker fileLocker = FileLocker.tryLock( LOCK_FILE_NAME ) )
        {
        	Assert.assertTrue( new File( LOCK_FILE_NAME ).exists() );
	        boolean exit = false;
	        int  counter = 0;
	        
	        do
	        {
	        	try
	        	{
	        		counter++;
	        		FileLocker.tryLock( LOCK_FILE_NAME );
	        		
	        		exit = true;
	        	} catch( FileLockException e )
	        	{
	        		log.info( "failed to aquire lock on " + counter + " attempt" );
	        		break;
	        	}
	        	
	        }while( !exit );
        }
        
        Assert.assertFalse( new File( LOCK_FILE_NAME ).exists() );
	}
}
