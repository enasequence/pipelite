package uk.ac.ebi.ena.sra.pipeline.launcher;

public interface 
LauncherLockManager extends AutoCloseable
{
	boolean tryLock( String lock_id );
	boolean unlock( String lock_id );
	void purge( String allocator_name );
	void purgeDead() throws InterruptedException;
    boolean isLocked( String lock_id );
    boolean terminate( String lock_id );
    boolean isBeingHeld( String lock_id );
}
