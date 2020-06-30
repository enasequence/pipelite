package uk.ac.ebi.ena.sra.pipeline.launcher;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.sql.Timestamp;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteState.State;
import uk.ac.ebi.ena.sra.pipeline.launcher.StageExecutor.ExecutionInfo;
import uk.ac.ebi.ena.sra.pipeline.launcher.iface.ExecutionResult;
import uk.ac.ebi.ena.sra.pipeline.launcher.iface.Stage;
import uk.ac.ebi.ena.sra.pipeline.resource.MemoryLocker;
import uk.ac.ebi.ena.sra.pipeline.storage.StorageBackend;
import uk.ac.ebi.ena.sra.pipeline.storage.StorageBackend.StorageException;


public class 
ProcessLauncherTest
{
    
    private static final String MOCKED_PIPELINE = "MOCKED PIPELINE";
    static Logger log = Logger.getLogger( ProcessLauncherTest.class );


    class 
    StageInstanceInitializer
    {
        final String    stageName;
        final String    execMessage;
        final Timestamp execDate;
        final int       exec_cnt;
        final boolean   enabled;
        
        public 
        StageInstanceInitializer( String stageName,
                                  String execMessage,
                                  Timestamp execDate,
                                  int exec_cnt,
                                  boolean enabled )
        {
            this.stageName   = stageName;
            this.execMessage = execMessage;
            this.execDate    = execDate;
            this.exec_cnt    = exec_cnt;
            this.enabled     = enabled;
        }
        
    }
    
    
    
    @BeforeClass public static void
    setup()
    {
        PropertyConfigurator.configure( "resource/test.log4j.properties" );
    }
    
    
    enum 
    ERESULTS implements ExecutionResult
    { 
        OK(        ExecutionResult.RESULT_TYPE.SUCCESS, 0 ), 
        PERMANENT( ExecutionResult.RESULT_TYPE.PERMANENT_ERROR, 1 ), 
        TRANSIENT( ExecutionResult.RESULT_TYPE.TRANSIENT_ERROR, 2 ), 
        PSHPSH(    ExecutionResult.RESULT_TYPE.SKIPPED, 3 );
        
        
        final public ExecutionResult.RESULT_TYPE type;
        final public int code;        


        ERESULTS( RESULT_TYPE type, int code )
        {
            this.type = type;
            this.code = code;
        }


        @Override public RESULT_TYPE
        getType()
        {
            return type;
        }

        @Override public byte
        getExitCode()
        {
            return (byte)code;
        }

        @Override public Class<Throwable>
        getCause()
        {
           return null;
        }

        @Override public String
        getMessage()
        {
            return this.toString();
        } 
    };
    

    private StorageBackend
    initStorage( final String[] names, 
                 final ERESULTS[] init_results,
                 final boolean[] enabled ) throws StorageException
    {
        StorageBackend  mockedStorage = mock( StorageBackend.class );
        final PipeliteState stored_state = new PipeliteState();
        stored_state.setPipelineName( MOCKED_PIPELINE );
        stored_state.setProcessId( "YOBA-PROCESS" );
        stored_state.setState( State.ACTIVE );
        stored_state.setExecCount( 0 );
        stored_state.setPriority( 1 );
        stored_state.setProcessComment( "PSHPSH! ALO YOBA ETO TY?" );

        doAnswer( 
                    new Answer<Object>()
                    {
                        final AtomicInteger counter = new AtomicInteger();
                        
                        public Object 
                        answer( InvocationOnMock invocation ) 
                        {
                            StageInstance si = (StageInstance)invocation.getArguments()[ 0 ];
                            si.setEnabled( true );
                            si.setExecutionCount( 0 );
                            si.setProcessID( "YOBA-PROCESS" );
                            si.setStageName( names[ counter.getAndAdd( 1 ) ] );
                            si.setPipelineName( MOCKED_PIPELINE );
                            si.getExecutionInstance().setStartTime( new Timestamp( System.currentTimeMillis() ) );
                            si.getExecutionInstance().setFinishTime( new Timestamp( System.currentTimeMillis() ) );
                            si.getExecutionInstance().setResult( init_results[ counter.get() - 1 ].toString() );
                            si.getExecutionInstance().setResultType( init_results[ counter.get() - 1 ].getType() );
                            
                            si.setDependsOn( 1 == counter.get() ? null : names[ counter.get() - 2 ] );
                            
                            si.setEnabled( enabled[ counter.get() - 1 ] );
                            if( counter.get() >= names.length )
                            	counter.set( 0 );
                            
                            return null;
                        } 
                    } ).when( mockedStorage ).load( any( StageInstance.class ) );

        doAnswer( 
                new Answer<Object>()
                {
                    public Object 
                    answer( InvocationOnMock invocation ) 
                    {
                        PipeliteState si = (PipeliteState)invocation.getArguments()[ 0 ];
                        si.setPipelineName( stored_state.getPipelineName() );
                        si.setProcessId( stored_state.getProcessId() );
                        si.setState( stored_state.getState() );
                        si.setPriority( stored_state.getPriority() );
                        si.setExecCount( stored_state.getExecCount() );
                        si.setProcessComment( stored_state.getProcessComment() );
                        return null;
                    } 
                } ).when( mockedStorage ).load( any( PipeliteState.class ) );
        
        doAnswer( 
                new Answer<Object>()
                {
                    public Object 
                    answer( InvocationOnMock invocation ) 
                    {
                        PipeliteState si = (PipeliteState)invocation.getArguments()[ 0 ];

                        stored_state.setPipelineName( si.getPipelineName() );
                        stored_state.setProcessId( si.getProcessId() );
                        stored_state.setState( si.getState() );
                        stored_state.setPriority( si.getPriority() );
                        stored_state.setExecCount( si.getExecCount() );
                        stored_state.setProcessComment( si.getProcessComment() );

                        return null;
                    } 
                } ).when( mockedStorage ).save( any( PipeliteState.class ) );
        
        return mockedStorage;
    }


    private ProcessLauncher
    initProcessLauncher( Stage[] stages, ExecutionResult[] results, StorageBackend storage, StageExecutor executor )
    {
        ProcessLauncher process = spy( new ProcessLauncher() );
        process.setProcessID( "TEST_PROCESS" );
        process.setStorage( storage );
        process.setExecutor( executor );
        process.setStages( stages );
        process.setCommitStatuses( results );
        return process;
    }


    private StageExecutor
    initExecutor( ExecutionResult[] results, int...invocation_exit_code )
    {
        StageExecutor spiedExecutor = spy( new InternalStageExecutor( new ResultTranslator( results ) ) );
        final AtomicInteger inv_cnt = new AtomicInteger( 0 );
        doAnswer( new Answer<Object>()
                  {
                    public Object
                    answer( InvocationOnMock i )
                    {
                        StageInstance si = (StageInstance) i.getArguments()[ 0 ];
                        log.info( "Calling execute on \"" + si.getStageName() + "\"" );
                        return null;
                    }
                  } ).when( spiedExecutor ).execute( any( StageInstance.class ) );
        
        doAnswer( new Answer<Object>()
                {
                  public Object
                  answer( InvocationOnMock i )
                  {
                	  ExecutionInfo info = new ExecutionInfo();
                      info.setExitCode( invocation_exit_code.length > inv_cnt.get() ? invocation_exit_code[ inv_cnt.getAndIncrement() ] : 0  );
                      info.setThrowable( null );
                      info.setCommandline( "Command Line" );
                      info.setStderr( "Stderr" );
                      info.setStdout( "Stdout" );
                      return info;
                  }
                } ).when( spiedExecutor ).get_info();

        return spiedExecutor;
    }


    
    @Test public void
    Test() throws StorageException
    {

        Stage[] stages = new Stage[] { mock( Stage.class ), mock( Stage.class ), mock( Stage.class ), mock( Stage.class ) };

        {
            StorageBackend mockedStorage = initStorage( new String[]   { "1: SOVSE MALI YOBA", "2: MALI YOBA", "3: BOLSHE YOBA",      "4: OCHE BOLSHE YOBA" }, 
                                                        new ERESULTS[] { ERESULTS.OK,          ERESULTS.OK,    ERESULTS.TRANSIENT,    ERESULTS.PSHPSH },
                                                        new boolean[]  { false,                true,           true,                  true } );
            
            StageExecutor spiedExecutor = initExecutor( ERESULTS.values(), new int[] { 0, 2, 0, 2 } );
            ProcessLauncher pl = initProcessLauncher( stages, ERESULTS.values(), mockedStorage, spiedExecutor );
            pl.setLocker( new MemoryLocker() );
            pl.setRedoCount( 2 );
            pl.lifecycle();
            
            verify( pl, times( 1 ) ).lifecycle();
            verify( spiedExecutor, times( 2 ) ).execute( any( StageInstance.class ) );
            
            Assert.assertEquals( State.ACTIVE, pl.state.getState() );
            Assert.assertEquals( 1, pl.state.getExecCount() );
            
            //Re-run
            pl.lifecycle();

            verify( pl, times( 2 ) ).lifecycle();
            verify( spiedExecutor, times( 4 ) ).execute( any( StageInstance.class ) );
            Assert.assertEquals( State.FAILED, pl.state.getState() );
            Assert.assertEquals( 2, pl.state.getExecCount() );
            
        }

        {
            StorageBackend mockedStorage = initStorage( new String[]   { "SOVSE MALI YOBA",  "MALI YOBA", "BOLSHE YOBA",      "OCHE BOLSHE YOBA" }, 
                                                        new ERESULTS[] { ERESULTS.PERMANENT, ERESULTS.OK, ERESULTS.TRANSIENT, ERESULTS.PSHPSH },
                                                        new boolean[]  { false,              false,       true,               true } );
            
            StageExecutor spiedExecutor = initExecutor( ERESULTS.values() );
            ProcessLauncher pl = initProcessLauncher( stages, ERESULTS.values(), mockedStorage, spiedExecutor );
            pl.setLocker( new MemoryLocker() );
            pl.lifecycle();
            
            verify( pl, times( 1 ) ).lifecycle();
            verify( spiedExecutor, times( 2 ) ).execute( any( StageInstance.class ) );
        };


        {
            StorageBackend mockedStorage = initStorage( new String[]   { "SOVSE MALI YOBA", "MALI YOBA", "BOLSHE YOBA",      "OCHE BOLSHE YOBA" }, 
                                                        new ERESULTS[] { ERESULTS.OK,       ERESULTS.OK, ERESULTS.TRANSIENT, ERESULTS.PSHPSH },
                                                        new boolean[]  { false,             true,        true,               true } );
            
            StageExecutor spiedExecutor = initExecutor( ERESULTS.values() );
            ProcessLauncher pl = initProcessLauncher( stages, ERESULTS.values(), mockedStorage, spiedExecutor );
            pl.setLocker( new MemoryLocker() );
            pl.lifecycle();
            
            verify( pl, times( 1 ) ).lifecycle();
            verify( spiedExecutor, times( 2 ) ).execute( any( StageInstance.class ) );
        }
    
    
        {
            StorageBackend mockedStorage = initStorage( new String[]   { "SOVSE MALI YOBA",  "MALI YOBA", "BOLSHE YOBA",      "OCHE BOLSHE YOBA" }, 
                                                        new ERESULTS[] { ERESULTS.PERMANENT, ERESULTS.OK, ERESULTS.TRANSIENT, ERESULTS.PSHPSH },
                                                        new boolean[]  { true,               true,        true,               true } );
            
            StageExecutor spiedExecutor = initExecutor( ERESULTS.values() );
            ProcessLauncher pl = initProcessLauncher( stages, ERESULTS.values(), mockedStorage, spiedExecutor );
            pl.setLocker( new MemoryLocker() );
            pl.lifecycle();

            verify( pl, times( 1 ) ).lifecycle();
            verify( spiedExecutor, times( 0 ) ).execute( any( StageInstance.class ) );
            
            Assert.assertEquals( State.FAILED, pl.state.getState() );
        }

        {
            StorageBackend mockedStorage = initStorage( new String[]   { "SOVSE MALI YOBA",  "MALI YOBA", "BOLSHE YOBA",      "OCHE BOLSHE YOBA" }, 
                                                        new ERESULTS[] { ERESULTS.TRANSIENT, ERESULTS.OK, ERESULTS.TRANSIENT, ERESULTS.PSHPSH },
                                                        new boolean[]  { true,               true,        true,               true } );
            
            StageExecutor spiedExecutor = initExecutor( ERESULTS.values() );
            ProcessLauncher pl = initProcessLauncher( stages, ERESULTS.values(), mockedStorage, spiedExecutor );
            pl.setLocker( new MemoryLocker() );
            pl.lifecycle();
            
            verify( pl, times( 1 ) ).lifecycle();
            verify( spiedExecutor, times( 4 ) ).execute( any( StageInstance.class ) );
            
            Assert.assertEquals( State.COMPLETED, pl.state.getState() );
        }
    }
}
