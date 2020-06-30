package uk.ac.ebi.ena.sra.pipeline.launcher;

import uk.ac.ebi.ena.sra.pipeline.base.external.ExternalCall;

public class 
SimpleBackEnd extends ExternalCall implements ExternalCallBackEnd
{

    @Override
    public ExternalCall 
    new_call_instance( String job_name, final String executable, final String[] args )
    {
        return (ExternalCall) new ExternalCall() 
        {
            {
                setExecutable( executable );
                setArgs( args );
            }
        };
    }
    
    
    
}
