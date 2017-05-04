package uk.ac.ebi.ena.sra.pipeline.resource;

import org.junit.Assert;
import org.junit.Test;

public class
MemoryLockerTest
{

    @Test public void 
    Test()
    {
        MemoryLocker ml = new MemoryLocker();
        ResourceLock rl = new ResourceLock( "1", "Я" );
        Assert.assertTrue( ml.lock( new ResourceLock( "1", "Я" ) ) );
        Assert.assertTrue( ml.is_locked( new ResourceLock( "1", "Я" ) ) );
        Assert.assertTrue( ml.lock( new ResourceLock( "2", "Я" ) ) );
        Assert.assertTrue( ml.is_locked( new ResourceLock( "2", "Я" ) ) );
        Assert.assertTrue( ml.unlock( rl ) );
        Assert.assertFalse( ml.is_locked( new ResourceLock( "1", "Я" ) ) );
        Assert.assertTrue( ml.is_locked( new ResourceLock( "2", "Я" ) ) );
    }
}
