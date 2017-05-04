package uk.ac.ebi.ena.sra.pipeline.storage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import uk.ac.ebi.ena.sra.pipeline.launcher.StageInstance;
import uk.ac.ebi.ena.sra.pipeline.launcher.iface.Stage;
import uk.ac.ebi.ena.sra.pipeline.launcher.iface.StageTask;
import uk.ac.ebi.ena.sra.pipeline.storage.EnumStorage.ProcessIdFactory;
import uk.ac.ebi.ena.sra.pipeline.storage.StorageBackend.StorageException;

public class 
EnumStorageTest
{
    static Logger log = Logger.getLogger( EnumStorageTest.class );
    
    @BeforeClass public static void
    beforeClass()
    {
        PropertyConfigurator.configure( "resource/test.log4j.properties" );
    }
    
    
    public static enum 
    __testovye_shagee implements Stage
    {
        ОДИН,
        ДВА,
        ТРИ;

        
        @Override public Class<? extends StageTask>
        getTaskClass()
        {
            return ( new StageTask() 
            {                
                @Override public void
                unwind()
                {
            
                }
                
                
                @Override public void
                init( Object id, boolean do_commit ) throws Throwable
                {

                }
                
                
                @Override public void
                execute() throws Throwable
                {

                }
            } ).getClass();
        }

        
        @Override public Stage 
        getDependsOn()
        {
            return null;
        }

        
        @Override public String
        getDescription()
        {
            return toString();
        }
        
    }
    
    
    
    @Test public void
    test() throws StorageException, NoSuchFieldException
    {
        EnumStorage<__testovye_shagee> es = new EnumStorage<__testovye_shagee>( __testovye_shagee.class );
        es.setProcessIdFactory( new ProcessIdFactory() { public String getProcessId() { return "ИДЕНТИФИКАТОР"; } } );
        es.setPipelineName( "ТЕСТОВАЯ_ЛИНИЯ" );
        List<StageInstance> si_list = new ArrayList<>();
        Stream.of( __testovye_shagee.values() ).forEach( stage -> {
            try
            {
                StageInstance si = new StageInstance();
                si.setStageName( stage.toString() );
                es.load( si );
                log.info( si );
                si_list.add( si );
            } catch( StorageException se )
            {
                throw new RuntimeException( se );
            }
        } );
        log.info( Arrays.asList( si_list ) );
        Assert.assertEquals( __testovye_shagee.values().length, si_list.size() );
        
        ProcessLogBean lb = Mockito.spy( new ProcessLogBean() );
        
        lb.setPipelineName( es.getPipelineName() );
        es.save( lb );
        
        Mockito.verify( lb, Mockito.atLeastOnce() ).getPipelineName();
    }
}
