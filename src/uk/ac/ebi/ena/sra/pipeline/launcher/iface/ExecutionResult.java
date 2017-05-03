package uk.ac.ebi.ena.sra.pipeline.launcher.iface;



public interface 
ExecutionResult 
{
    public enum 
    RESULT_TYPE
    {
        SUCCESS( false, false ),
        SKIPPED( false, false ),
        TRANSIENT_ERROR( true, true ), /* consider ERROR */
        PERMANENT_ERROR( true, false ); /* consider FATAL */
        
        
        RESULT_TYPE( boolean is_failure, boolean can_reprocess )
        {
            this.is_failure = is_failure;
            this.can_reprocess = can_reprocess;
        }
        
        
        final boolean is_failure;
        final boolean can_reprocess;
        
        
        public boolean
        isFailure()
        {
            return is_failure;
        }
        
        
        public boolean
        canReprocess()
        {
            return can_reprocess;
        }
    };
    
    public RESULT_TYPE        getType();
    public byte               getExitCode(); // exit code for status. unique.
    public Class<Throwable>   getCause();    // throwable that can cause the status. unique.
    public String             getMessage();  // commit message. unique.
}
