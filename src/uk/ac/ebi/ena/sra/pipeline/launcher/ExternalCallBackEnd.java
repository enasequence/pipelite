package uk.ac.ebi.ena.sra.pipeline.launcher;

import uk.ac.ebi.ena.sra.pipeline.base.external.ExternalCall;

public interface 
ExternalCallBackEnd
{
     ExternalCall new_call_instance( String job_name, String executable, String[] args );
}
