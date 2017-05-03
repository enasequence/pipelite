package uk.ac.ebi.ena.sra.pipeline.launcher;

import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncher.PipeliteProcess;

public abstract class 
ProcessPoolExecutor extends TaggedPoolExecutor
{
    public 
    ProcessPoolExecutor( int corePoolSize )
    {
        super( corePoolSize );
    }

    
    @Override
    @Deprecated
    public void
    execute( Object id, Runnable runnable )
    {
        super.execute( id, runnable );
    }
    
    
    @Override
    public void 
    execute( Runnable process )
    {
        execute( ( (PipeliteProcess)process).getProcessId(), process );
    }

    
    public abstract void
    unwind( PipeliteProcess process );

    public abstract void
    init( PipeliteProcess r );

    
    @Override
    protected void 
    afterExecute( Runnable r, Throwable t )
    {
        unwind( (PipeliteProcess) r );
        super.afterExecute( r, t );
    }

    
    @Override
    protected void 
    beforeExecute( Thread t, Runnable r )
    {
        super.beforeExecute( t, r );
        init( (PipeliteProcess) r );
    }
}
