package uk.ac.ebi.ena.sra.pipeline.filelock;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import sun.misc.Cleaner;



//PID@MACHINE
public class 
FileLockManager implements AutoCloseable
{
	private static final long FILE_LOCK_LENGTH = 666;
	static Logger log = Logger.getLogger( FileLockManagerTest.class );
	private int port = 0;
	private Pong pong;
	ExecutorService e = Executors.newSingleThreadExecutor();
	
	
	
	public
	FileLockManager() throws InterruptedException
	{
		this( 0 );
	}
	
	
	public
	FileLockManager( int port ) throws InterruptedException
	{
		this.pong = new Pong( this.port, ManagementFactory.getRuntimeMXBean().getName() );
		e.submit( this.pong );
		this.port = pong.getPort(); 
	}
	
	
	public int 
	getPort()
	{
		return this.port;
	}
	

	public static boolean 
	unlock( String path )
	{
		return false;
	}
	
	
	private static FileLockInfo
	parseFileLock( String line )
	{
		Pattern lock_pattern = Pattern.compile( "([\\d]+)@([^:]+):([\\d]+)" );
		Matcher m = lock_pattern.matcher( line );
		if( m.matches() )
		{
			log.info( "To parse: " + line );
			return new FileLockInfo( null, m.group( 1 ) + "@" + m.group( 2 ), m.group( 2 ), Integer.parseInt( m.group( 3 ) ) );
		}
		
		return null;
	}
	
	
	private static String
	formFileLock( FileLockInfo info )
	{
		return String.format( "%s:%s", info.machine_id, info.port );
	}
	
	
	//TODO remove lock files
	public boolean 
	tryLock( String path )
	{
		File lockFile = new File( path ).getAbsoluteFile();
		
        log.info( "locking on " + lockFile );
        
        try
        {
        	//Demand file not exist for at least 30 sec 
        	for( int attempt = 0; attempt < 3 && !lockFile.exists(); ++attempt )
        		Thread.sleep( 10 * 1000 );
        	
        	if( !lockFile.getParentFile().exists() && !lockFile.getParentFile().mkdirs() )
            	throw new FileLockException( new FileLockInfo( lockFile.getPath(), null, null, 0 ), "Failed to create lock parent folders", null );

        	try( RandomAccessFile raf = new RandomAccessFile( lockFile, "rws" );            
        		 FileChannel      fc  = raf.getChannel();
        	     FileLock         fl  = fc.tryLock( 0L, Long.MAX_VALUE, false ) )
        	{
        		MappedByteBuffer out = fc.map( FileChannel.MapMode.READ_WRITE, 0, FILE_LOCK_LENGTH );
        		ByteArrayOutputStream os = new ByteArrayOutputStream();
        		ByteBuffer read = out.duplicate();
        		for( ; read.remaining() > 0; )
        		{
        			byte ch = read.get();
        			if( ch != 0 )
        				os.write( ch );
        		}
        		
        		FileLockInfo info = parseFileLock( os.toString( StandardCharsets.UTF_8.toString() ) );
        		
        		if( null != info )
        		{
        			info = new FileLockInfo( path, info.machine_id, info.machine, info.port );
        			if( pingLockOwner( info ) )
        			{
        				log.info( "Busy: " + info );			
        				return false;
        			}
        		}

        		out.put( ( ManagementFactory.getRuntimeMXBean().getName() + ":" + String.valueOf( port ) ).getBytes( StandardCharsets.UTF_8 ) );
        		Cleaner cleaner = ((sun.nio.ch.DirectBuffer) out).cleaner();
        		if( cleaner != null )
        			cleaner.clean();
        	}
        } catch( OverlappingFileLockException | IOException | InterruptedException e1 )
        {
            throw new FileLockException( new FileLockInfo( lockFile.getPath(), null, null, 0 ), "Failed to create lock file.", e1 );
        }
        
        return true;
    }

	
	
	
	private class 
	Pong implements Runnable
	{
		ServerSocket server;
		int port;
		CountDownLatch l = new CountDownLatch( 1 );
		final String machine_id;
		
		volatile boolean stop = false;
		
		
		Pong( int port, String machine_id )
		{
			this.port = port;
			this.machine_id  = machine_id;
		}

		
		public int
		getPort() throws InterruptedException
		{
			l.await();
			return this.port;
		}
		
		
		public void
		stop()
		{
			this.stop = true;
		}
		
		@Override public void 
		run()
		{
			try 
			{
				this.server = new ServerSocket( this.port );
				this.port   = server.getLocalPort();
				l.countDown();
				
				while( !stop )
				{
				   Socket client = server.accept();
				   //TODO multi-thread?
				   try ( BufferedReader   input_reader  = new BufferedReader( new InputStreamReader( client.getInputStream() ) );
						 DataOutputStream client_stream = new DataOutputStream( client.getOutputStream() ) )
				   {
					   String request_line = input_reader.readLine();
					   FileLockInfo info = FileLockManager.this.parseFileLock( request_line );
					   String reply = String.valueOf( null == info ? Boolean.FALSE : this.machine_id.equals( info.machine_id ) ? Boolean.TRUE : Boolean.FALSE ) + "\n";
					   client_stream.writeBytes( reply );
					   log.info( "recv: " + request_line + ", resp: " + reply );
				   } catch( Throwable t )
				   {
					   log.error( "Pong", t );
				   } finally
				   {
					   client.close();
				   }

				}
			} catch (IOException e) 
			{
			
				e.printStackTrace();
			}			
			
		}
		
	}
	
	
	public static boolean
	pingLockOwner( FileLockInfo info ) throws UnknownHostException, IOException
	{
		try( Socket kkSocket = new Socket( info.machine, info.port );
			 PrintWriter out = new PrintWriter( kkSocket.getOutputStream(), true );
		     BufferedReader in = new BufferedReader( new InputStreamReader( kkSocket.getInputStream() ) ); )
		{
			out.println( FileLockManager.formFileLock( info ) );
			return Boolean.parseBoolean( in.readLine() );
			
		} catch( ConnectException e )
		{
			log.info( "cannot ping " + info );
			return false;
		}


	}


	@Override public void 
	close() throws Exception 
	{
		this.e.shutdown();
	}
}
