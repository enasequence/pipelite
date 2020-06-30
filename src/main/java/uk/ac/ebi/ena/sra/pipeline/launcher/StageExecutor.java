package uk.ac.ebi.ena.sra.pipeline.launcher;

import uk.ac.ebi.ena.sra.pipeline.base.common.CompressedString;
import uk.ac.ebi.ena.sra.pipeline.executors.ExecutorConfig;

public interface 
StageExecutor
{
    // Existing statuses:
    // 1 unknown /not processed.  StageTransient
    // 2 permanent success.       StageTerminal
    // 3 transient success.       StageTransient
    // 4 permanent failure.       ProcessTerminal
    // 5 transient failure.       StageTransient
    // 6 >ExecutionCounter.       ProcessTerminal

    enum 
    EvalResult
    {
        StageTransient, // stage can be executed.
        StageTerminal,  // stage cannot be executed.
        ProcessTerminal // stage disables parent process.
    }
    
    
    public class 
    ExecutionInfo
    {
        private String  host;
        private Integer pid;
        private String  commandline;
        private CompressedString stderr;
        private CompressedString stdout;
        private Throwable t;
        private Integer exit_code;
        private CompressedString log_message;
        

        public String 
        getHost()
        {
            return host;
        }
        
        
        public void 
        setHost( String host )
        {
            this.host = host;
        }
        
        
        public Integer 
        getPID()
        {
            return pid;
        }
        
        
        public void 
        setPID( Integer pid )
        {
            this.pid = pid;
        }
        
        
        public String 
        getCommandline()
        {
            return commandline;
        }
        
        
        public void 
        setCommandline( String commandline )
        {
            this.commandline = commandline;
        }
        
        
        public String 
        getStderr()
        {
            return null == stderr ? null : stderr.toString();
        }
        
        
        public void
        setStderr( String stderr )
        {
            this.stderr = new CompressedString( stderr );
        }
        
        
        public void
        setStderr( CompressedString stderr )
        {
            this.stderr = stderr;
        }
        
        
        public String 
        getStdout()
        {
            return null == stdout ? null : stdout.toString();
        }
        
        
        public void 
        setStdout( String stdout )
        {
            this.stdout = new CompressedString( stdout );
        }
        
        
        public void 
        setStdout( CompressedString stdout )
        {
            this.stdout = stdout;
        }

        
        public Throwable 
        getThrowable()
        {
            return t;
        }
        
        
        public void 
        setThrowable( Throwable t )
        {
            this.t = t;
        }


        public void 
        setExitCode( Integer exit_code )
        {
            this.exit_code = exit_code;
        }


        public Integer 
        getExitCode()
        {
            return exit_code;
        }


		public String 
		getLogMessage() 
		{
			return null ==  log_message ? null : log_message.toString();
		}


		public void 
		setLogMessage( String log_message )
		{
			this.log_message = new CompressedString( log_message );
		}
    }

    
    void          reset( StageInstance instance );
    void          execute( StageInstance instance );
    <T extends ExecutorConfig> void configure( T params );
    EvalResult    can_execute( StageInstance instance );
//    boolean       was_error();
    ExecutionInfo get_info();
    
    void setClientCanCommit( boolean do_commit );
    boolean getClientCanCommit();
    Class<? extends ExecutorConfig> getConfigClass();
}