package uk.ac.ebi.ena.sra.pipeline.launcher;

import java.nio.file.Path;
import java.nio.file.Paths;
import uk.ac.ebi.ena.sra.pipeline.base.external.LSFClusterCall;
import uk.ac.ebi.ena.sra.pipeline.base.external.LSFClusterCall.LSFQueue;

public class 
LSFBackEnd implements ExternalCallBackEnd
{
    String queue;
    int memory_limit;
    int memory_reservation_timeout;
    int cpu_cores;
    private Path output_path;
    
    
    @Override public LSFClusterCall
    new_call_instance( String job_name, final String executable, final String args[] )
    {
        return new_call_instance( job_name, 
                                  executable, 
                                  args, 
                                  memory_limit, 
                                  memory_reservation_timeout, 
                                  cpu_cores );
    }
    
    
    public LSFClusterCall
    new_call_instance( String job_name, 
                       final String executable, 
                       final String args[], 
                       int memory_limit, 
                       int memory_reservation_timeout, 
                       int cpu_cores )
    {
        LSFClusterCall call = new LSFClusterCall( output_path )
        {
            {
                setExecutable( executable );
                setArgs( args );
            }
        };

        call.setCPUNumber( cpu_cores );
        call.setMemoryLimit( memory_limit );
        call.setMemoryReservationTimeout( memory_reservation_timeout );
        call.setJobName( job_name );
        call.setQueue( queue );
        return call;
    }

    
    public 
    LSFBackEnd( String queue_name, int default_memory_limit, int default_memory_reservation_timeout, int default_cpu_cores )
    {
        this.queue = queue_name;
        this.memory_limit = default_memory_limit;
        this.memory_reservation_timeout = default_memory_reservation_timeout;
        this.cpu_cores = default_cpu_cores;
        this.output_path = Paths.get( System.getProperty( "java.io.tmpdir" ) );
    }
    
    
    public void 
    setOutputFolderPath( Path output_path )
    {
    	this.output_path = output_path; 
    }
}
