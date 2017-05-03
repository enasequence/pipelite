package uk.ac.ebi.ena.sra.pipeline.launcher;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import uk.ac.ebi.ena.sra.pipeline.configuration.DefaultConfiguration;
import uk.ac.ebi.ena.sra.pipeline.launcher.iface.StageTask;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

public class StageLauncher
{
    final static PatternLayout layout                       = new PatternLayout( "%d{ISO8601} %-5p [%t] %c{1}:%L - %m%n" );
    protected Connection       connection;

    public static final String PARAMETERS_NAME_ID           = "--id";
    public static final String PARAMETERS_NAME_STAGE        = "--stage";
    public static final String PARAMETERS_NAME_FORCE_COMMIT = "--commit";
    public static final String PARAMETERS_NAME_ENABLED      = "--enabled";
    public static final String PARAMETERS_NAME_EXEC_COUNT   = "--exec-cnt";
    

    
    @Parameter( names = PARAMETERS_NAME_ID, description = "ID of the task", required = true )
    String process_id;

    @Parameter( names = PARAMETERS_NAME_STAGE, description = "Stage name to execute", required = true )
    String stage_name;

    @Parameter( names = PARAMETERS_NAME_FORCE_COMMIT, description = "force commit client data" )
    boolean force;


    //hidden
    @Parameter( names = PARAMETERS_NAME_ENABLED, description = "stage is enabled", hidden=true, arity=1 )
    boolean enabled = true;
    //hidden
    @Parameter( names = PARAMETERS_NAME_EXEC_COUNT, description = "execution counter", hidden=true )
    int    exec_cnt = 0;
    
    
    public static void
    main( String[] args ) throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException
    {
        ConsoleAppender appender = new ConsoleAppender( layout, "System.out" );
        appender.setThreshold( Level.INFO );

        String os = System.getProperty( "os.name" );

        Logger.getRootLogger().removeAllAppenders();
        Logger.getRootLogger().addAppender( appender );
        Logger.getRootLogger().setLevel( Level.ALL );

        Logger.getRootLogger().warn( "Operating system is " + os );

        StageLauncher sl = new StageLauncher();
        JCommander jc = new JCommander( sl );

        try
        {
            jc.parse( args );
            System.exit( sl.execute() );

        } catch( ParameterException pe )
        {

            jc.usage();
            System.exit( 1 );
        }
    }


    public int
    execute()
    {
        StageExecutor executor = new InternalStageExecutor( new ResultTranslator( DefaultConfiguration.currentSet().getCommitStatus() ) )
                                    .setReprocessProcessed( true )
                                    .setRedoCount( DefaultConfiguration.currentSet().getStagesRedoCount() );
        
        StageInstance instance = new StageInstance();
        instance.setProcessID( process_id );
        instance.setStageName( stage_name );
        instance.setEnabled( enabled );
        instance.setExecutionCount( exec_cnt );
        executor.setClientCanCommit( force );
        executor.execute( instance );
        return executor.get_info().getExitCode();
    }


    public static class 
    InternalExecutionResult
    {
        public 
        InternalExecutionResult( int exitCode, StageTask task )
        {
            this.exitCode = exitCode;
            this.task = task;
        }


        public final int       exitCode;
        public final StageTask task;
    }


    public InternalExecutionResult
    execute( String process_id, String stage_name, boolean force )
    {
        InternalStageExecutor executor = new InternalStageExecutor( new ResultTranslator( DefaultConfiguration.currentSet().getCommitStatus() ) )
                                            .setReprocessProcessed( true )
                                            .setRedoCount( DefaultConfiguration.currentSet().getStagesRedoCount() );
        StageInstance instance = new StageInstance();
        instance.setProcessID( process_id );
        instance.setStageName( stage_name );
        instance.setEnabled( true );
        instance.setExecutionCount( 0 );
        executor.setClientCanCommit( force );
        executor.execute( instance );
        return new InternalExecutionResult( executor.get_info().getExitCode(), executor.get_task() );
    }
}
