package uk.ac.ebi.ena.sra.pipeline.base.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

public class 
FileLocker
{

    protected String   path;
    protected File     file;
    protected FileLock fileLock;
    protected long     timeout;
    
    
    FileLocker( LOCK_TYPE type, long timeout )
    {
        this( type.getFileName(), timeout );
    }
    
    
    private boolean 
    checkExists( File file )
    {
        boolean result = false;
        int     attempts = 3;
        do
        {
            result = ( file.isFile() || file.exists() );
            try
            {
                Thread.sleep( 1000 );
            } catch( InterruptedException ie )
            {
                ;
            }
        }while( !result && 0 < attempts-- );
        
        return result;
    }
    
    
    FileLocker( String path, long timeout )
    {
        File lockFile = new File( path ).getAbsoluteFile();
        if( timeout > 0 )
        {
            long end_time = System.currentTimeMillis() + timeout;
            while( checkExists( lockFile ) )
            {
                if( System.currentTimeMillis() > end_time )
                    throw new FileLockException( path, "Timeout. Lock file already exists.", null );
                try
                {
                    Thread.sleep( 1000 );
                } catch( InterruptedException e )
                {
                    ;
                }
            }
        } else if( timeout < 0 )
        {
            while( checkExists( lockFile ) )
                try
                {
                    Thread.sleep( 1000 );
                } catch( InterruptedException e )
                {
                    ;
                }
        } else
        {
            if( checkExists( lockFile ) )
                throw new FileLockException( path, "Lock file already exists.", null );
        }
        
        try
        {
            lockFile.getParentFile().mkdirs();
            if( !lockFile.createNewFile() )
            {
                throw new FileLockException( path, "Failed to create lock file or parent folders.", null );
            }
        } catch( IOException e1 )
        {
            throw new FileLockException( path, "Failed to create lock file.", e1 );
        }
        
        try
        {
            lockFile.deleteOnExit();

            FileOutputStream fos = null;
            try
            {
                fos = new FileOutputStream( lockFile );
            } catch( FileNotFoundException e1 )
            {
                throw new FileLockException( path, "Failed to open lock file.", e1 );
            }
            FileChannel lockFileChannel = fos.getChannel();
            try
            {
                fileLock = lockFileChannel.tryLock();
            } catch( IOException e1 )
            {
                throw new FileLockException( path, "Failed to lock the file.", e1 );
            }
            if( fileLock == null )
            {
                throw new FileLockException( path, "Failed to acquire file lock.", null );
            }
            try
            {
                fos.write( ManagementFactory.getRuntimeMXBean().getName().getBytes() );
                fos.flush();
            } catch( IOException e )
            {
                throw new FileLockException( path, "Failed to write into the locked file.", e );
            }
        } catch( Throwable t )
        {
            release();
            if( t instanceof FileLockException )
                throw (FileLockException) t;
            throw new FileLockException( path, "Unexpected exception while locking file.", t );
        }
    }
    
    
    public static synchronized FileLocker 
    tryLock( LOCK_TYPE type, long timeout ) throws FileLockException
    {
        return new FileLocker( type, timeout );
    }

    
    @Deprecated
    public static synchronized FileLocker 
    tryLock( String path ) throws FileLockException
    {
        return new FileLocker( path, 0 );
    }

    
    public synchronized void 
    release()
    {
        if( fileLock != null )
            try
            {
                fileLock.release();
            } catch( Throwable t )
            {
            }
        if( file != null && file.exists() )
            try
            {
                file.delete();
            } catch( Throwable t )
            {
            }
    }

    public String getPath()
    {
        return path;
    }


    enum
    LOCK_TYPE
    {
        MKDIR( System.getProperty( "user.home" ) + "/.era_mkdir.lock" );
        
        String f_name;
        LOCK_TYPE( String f_name )
        {
            this.f_name = f_name;
        }
        
        public String
        getFileName()
        {
            return f_name;
        }
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
}
