package uk.ac.ebi.ena.sra.pipeline.launcher;

import java.sql.Timestamp;

public interface 
LauncherLockManager extends AutoCloseable
{
	boolean tryLock( String lock_id );
	boolean unlock( String lock_id );
	void purge( Timestamp before_date );
    boolean isLocked( String lock_id );
    boolean terminate( String lock_id );
    boolean isBeingHeld( String lock_id );
}
