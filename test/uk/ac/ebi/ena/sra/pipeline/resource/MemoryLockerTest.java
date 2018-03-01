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
        StageResourceLock rl = new StageResourceLock( "NULL", "1", "1" );
        Assert.assertTrue( ml.lock( new StageResourceLock( "NULL", "1", "1" ) ) );
        Assert.assertTrue( ml.is_locked( new StageResourceLock( "NULL", "1", "1" ) ) );
        Assert.assertTrue( ml.lock( new StageResourceLock( "NULL", "2", "1" ) ) );
        Assert.assertTrue( ml.is_locked( new StageResourceLock( "NULL", "2", "1" ) ) );
        Assert.assertTrue( ml.unlock( rl ) );
        Assert.assertFalse( ml.is_locked( new StageResourceLock( "NULL", "1", "1" ) ) );
        Assert.assertTrue( ml.is_locked( new StageResourceLock( "NULL", "2", "1" ) ) );
    }
}
