package uk.ac.ebi.ena.sra.pipeline.launcher;

import uk.ac.ebi.ena.sra.pipeline.launcher.iface.ExecutionResult;

public class 
ResultTranslator
{
    final private ExecutionResult[] results;


    public 
    ResultTranslator( ExecutionResult[] results )
    {
        this.results = results;
    }

    
    private ExecutionResult 
    getCommitStatusOK()
    {
        return results[ 0 ];
    }

    
    protected ExecutionResult 
    getCommitStatusDefaultFailure()
    {
        return results[ results.length - 1 ];
    }

    
    protected ExecutionResult 
    getCommitStatus( Throwable t )
    {
        Class<?> klass = null == t ? null : t.getClass();
        
        for( ExecutionResult csd : results )
        {
            Class<? extends Throwable> cause = csd.getCause();
            if( klass == cause || ( null != cause && cause.isInstance( t ) ) )
                    return csd;
        }
        
        return getCommitStatusDefaultFailure();
    }

    
    protected ExecutionResult 
    getCommitStatus( int exit_code )
    {
        if( 0 == exit_code )
            return getCommitStatusOK();
        
        for( ExecutionResult csd : results )
        {
            if( exit_code == csd.getExitCode() )
                    return csd;
        }
        
        return getCommitStatusDefaultFailure();
    }

    
    protected ExecutionResult 
    getCommitStatus( String name )
    {
        for( ExecutionResult csd : results )
            if( name == csd.getMessage() || ( null != name && name.equals( csd.getMessage() ) ) )
                return csd;
    
        return getCommitStatusDefaultFailure();
    }
}
