package uk.ac.ebi.ena.sra.pipeline.base.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

import org.apache.log4j.Logger;

import sun.misc.Cleaner;

@Deprecated public class 
FileLocker implements AutoCloseable
{
	protected RandomAccessFile raf;
	protected FileChannel fc;
	protected FileLock fl;
	
    protected String   path;
    protected File     lockFile;
    private Logger log  = Logger.getLogger( getClass() );
    
	FileLocker( String path )
	{
		this.path = path;
		lockFile = new File( path ).getAbsoluteFile();
        log.info( "locking on " + lockFile );
        try
        {
        	if( !lockFile.getParentFile().exists() && !lockFile.getParentFile().mkdirs() )
            	throw new FileLockException( path, "Failed to create lock parent folders", null );
        	
        	Files.write( lockFile.toPath(), new byte[] {}, StandardOpenOption.APPEND, StandardOpenOption.CREATE, StandardOpenOption.DELETE_ON_CLOSE );
        	this.raf = new RandomAccessFile( lockFile, "rws" );            
        	this.fc  = raf.getChannel();
        	this.fl  = fc.tryLock( 0L, Long.MAX_VALUE, false );
        	        	

        	fc.truncate( 0 );
        	MappedByteBuffer out = fc.map( FileChannel.MapMode.READ_WRITE, 0, ManagementFactory.getRuntimeMXBean().getName().getBytes().length );
        	out.put( ManagementFactory.getRuntimeMXBean().getName().getBytes() );
        	Cleaner cleaner = ((sun.nio.ch.DirectBuffer) out).cleaner();
        	if( cleaner != null )
                cleaner.clean();

        } catch( OverlappingFileLockException | IOException e1 )
        {
            throw new FileLockException( path, "Failed to create lock file.", e1 );
        }
        
    }
    
    
    public static synchronized FileLocker 
    tryLock( String path ) throws FileLockException
    {
        return new FileLocker( path );
    }

    
    public synchronized void 
    release()
    {
    	if( this.fl != null )
        {
    		try
            {
                fl.release();
                raf.close();
            } catch( Throwable t )
            {
            	t.printStackTrace();
            }

        	try
            {
                lockFile.delete();
            } catch( Throwable t )
            {
            	t.printStackTrace();
            }
        }
    }

    
    public String 
    getPath()
    {
        return path;
    }


    public static class 
    FileLockException extends RuntimeException
    {
        private static final long serialVersionUID = 5424901065648705926L;
        private String            path;


        public FileLockException( String path, String message, Throwable cause )
        {
            super( message, cause );
            this.path = path;
        }

        public String getPath()
        {
            return path;
        }

        public void setPath( String path )
        {
            this.path = path;
        }
    }


	@Override public void 
	close() throws Exception 
	{
		release();
	}
}
