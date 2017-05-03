package uk.ac.ebi.ena.sra.pipeline.storage;

import uk.ac.ebi.ena.sra.pipeline.launcher.ExecutionInstance;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteState;
import uk.ac.ebi.ena.sra.pipeline.launcher.StageInstance;
import uk.ac.ebi.ena.sra.pipeline.storage.StorageBackend.StorageException;


public interface
StorageBackend
{
    class 
    StorageException extends Exception
    {
        public 
        StorageException()
        {
            super();
        }

        
        public 
        StorageException( String arg0, Throwable arg1 )
        {
            super( arg0, arg1 );
        }


        public 
        StorageException( String arg0 )
        {
            super( arg0 );
        }

        
        public 
        StorageException( Throwable arg0 )
        {
            super( arg0 );
        }


        private static final long serialVersionUID = 1L;
        
    }
    
    public void load( StageInstance si ) throws StorageException;
    public void save( StageInstance si ) throws StorageException;    
    
    public void load( PipeliteState ps ) throws StorageException;
    public void save( PipeliteState ps ) throws StorageException;
    
    public String getExecutionId() throws StorageException;
    public void   load( ExecutionInstance instance ) throws StorageException;
    public void   save( ExecutionInstance instance ) throws StorageException;
    
    
 //   void load( OracleProcessLogBean bean ) throws StorageException;
    void save( ProcessLogBean bean ) throws StorageException;
    void flush() throws StorageException;
    void close() throws StorageException;

}
