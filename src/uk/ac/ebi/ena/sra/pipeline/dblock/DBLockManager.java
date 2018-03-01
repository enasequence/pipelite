package uk.ac.ebi.ena.sra.pipeline.dblock;

import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;

import org.apache.log4j.Logger;

import uk.ac.ebi.ena.sra.pipeline.launcher.LauncherLockManager;
import uk.ac.ebi.ena.sra.pipeline.resource.ProcessResourceLock;
import uk.ac.ebi.ena.sra.pipeline.resource.ResourceLock;
import uk.ac.ebi.ena.sra.pipeline.resource.ResourceLocker;
import uk.ac.ebi.ena.sra.pipeline.resource.StageResourceLock;

public class 
DBLockManager implements LauncherLockManager, ResourceLocker 
{
	final private Connection connection;
	final private String pipeline_name;
	final private String allocator_name = ManagementFactory.getRuntimeMXBean().getName();
	final private Logger log = Logger.getLogger( this.getClass() );
	
	public
	DBLockManager( Connection connection, String pipeilne_name )
	{
		this.connection    = connection;
		this.pipeline_name = pipeilne_name;
	}
	
	

	@Override public void 
	close() throws Exception 
	{
	    purge( Timestamp.from( Instant.now() ) );
		connection.rollback();
		connection.close();
	}

	
	@Override public boolean
	tryLock( String lock_id ) 
	{
		try( PreparedStatement ps = connection.prepareStatement(
				" declare "
				+ " pragma autonomous_transaction; "
				+ " v_selected pls_integer; "
				+ " v_pipeline_name varchar2(255) := ?; "
				+ " v_lock_id varchar2(255) := ?; "
				+ " v_allocator_name varchar2(255) := ?; "
				+ " begin "
				+ " for v_cur in ( select rowid, rownum from pipelite_lock where pipeline_name = v_pipeline_name and lock_id is null ) " 
				+ " loop "
				+ " begin "
				+ " select v_cur.rownum into v_selected from pipelite_lock where rowid = v_cur.rowid for update skip locked; "
				+ " if v_selected is not null "
				+ " then "
				   /* dbms_output.put_line( 'rownum: ' || v_selected || ', rowid: ' || v_cur.rowid ); */
				+ "   if v_lock_id is not null "
				+ "   then "
				+ "     update pipelite_lock " 
				+ "        set lock_id = v_lock_id, "
				+ "            allocator_name = v_allocator_name " //SYS_CONTEXT( 'USERENV', 'OS_USER' ) || '@' || SYS_CONTEXT( 'USERENV', 'HOST' ) "
				+ "      where rowid = v_cur.rowid; "
				+ "     commit; "
				+ "    end if; "
				+ "   return; "
				+ "  end if; "
				+ " exception "
				+ "  when NO_DATA_FOUND THEN v_selected := null; "
				+ " end; "
				+ " end loop; "
				+ "  raise_application_error( -20001, 'Resource ' || v_pipeline_name || ' depleted' || case when v_lock_id is not null then ' or lock ' || v_lock_id || ' already exists' end ); "
				+ " end; " 
				 ) )
		{
			ps.setString( 1, this.pipeline_name );
			
			if( null == lock_id )
			{
				ps.setNull( 2, Types.VARCHAR );
			} else
			{
				ps.setString( 2, lock_id );
			}
			 ps.setString( 3, allocator_name );
			
			ps.execute();
			return true;
		} catch( SQLException e )
		{
			log.info( "ERROR: " + e.getMessage() );
//			if( 20001 == e.getErrorCode() )
//				e.getMessage();
			return false;
		}
	}

   
	@Override public boolean
	isLocked( String lock_id )
	{
       try( PreparedStatement ps = connection.prepareStatement(
                " declare "
                + " pragma autonomous_transaction; "
                + " v_pipeline_name varchar2(255) := ?; "
                + " v_lock_id varchar2(255) := ?; " 
                + " begin " 
                + "     for v_cur in ( select rowid, rownum from pipelite_lock where pipeline_name = v_pipeline_name and lock_id = v_lock_id ) " 
                + "     loop " 
                + "         return; "
                + "     end loop; "
                + "  raise_application_error( -20001, 'Lock ' || v_lock_id || ' not found for resource ' || v_pipeline_name ); "
                + " end; "
                 ) )
        {
            ps.setString( 1, this.pipeline_name );
            
            if( null == lock_id )
            {
                ps.setNull( 2, Types.VARCHAR );
            } else
            {
                ps.setString( 2, lock_id );
            }
            
            ps.execute();
            return true;
        } catch( SQLException e )
        {
            return false;
        }
	}
	
	
    @Override public boolean
    isBeingHeld( String lock_id )
    {
       try( PreparedStatement ps = connection.prepareStatement(
                " declare "
                + " pragma autonomous_transaction; "
                + " v_pipeline_name varchar2(255) := ?; "
                + " v_lock_id varchar2(255) := ?; " 
                + " v_allocator_name varchar2(255) := ?; "
                + " begin " 
                + "     for v_cur in ( select rowid, rownum "
                + "                      from pipelite_lock "
                + "                     where pipeline_name = v_pipeline_name "
                + "                       and lock_id = v_lock_id "
                + "                       and allocator_name = v_allocator_name ) " 
                + "     loop " 
                + "         return; "
                + "     end loop; "
                + "  raise_application_error( -20001, 'Lock ' || v_lock_id || ' not found for resource ' || v_pipeline_name ); "
                + " end; "
                 ) )
        {
            ps.setString( 1, this.pipeline_name );
            
            if( null == lock_id )
            {
                ps.setNull( 2, Types.VARCHAR );
            } else
            {
                ps.setString( 2, lock_id );
            }
            
            ps.setString( 3, allocator_name );
            ps.execute();
            return true;
        } catch( SQLException e )
        {
            return false;
        }
    }

	
	@Override public boolean 
	unlock( String lock_id ) 
	{
		try( PreparedStatement ps = connection.prepareStatement(
				" declare "
				+ " pragma autonomous_transaction; "
				+ " v_pipeline_name varchar2(255) := ?; "
				+ " v_lock_id varchar2(255) := ?; " 
                + " v_allocator_name varchar2(255) := ?; "
				+ " begin " 
				+ " for v_cur in ( select rowid, rownum "
				+ "                  from pipelite_lock "
				+ "                 where pipeline_name = v_pipeline_name "
				+ "                   and lock_id = v_lock_id "
				+ "                   and allocator_name = v_allocator_name "
				+ "                   for update skip locked ) " 
				+ " loop " 
				+ "  update pipelite_lock " 
				+ "     set lock_id = null, "
				+ "         allocator_name = null "
				+ "   where rowid = v_cur.rowid; "
				+ "  commit; "
				+ "  return; "
				+ " end loop; "
				+ "  raise_application_error( -20001, 'Lock ' || v_lock_id || ' not found for resource ' || v_pipeline_name ); "
				+ " end; "
				 ) )
		{
			ps.setString( 1, this.pipeline_name );
			
			if( null == lock_id )
			{
				ps.setNull( 2, Types.VARCHAR );
			} else
			{
				ps.setString( 2, lock_id );
			}
			ps.setString( 3, allocator_name );
			ps.execute();
			return true;
		} catch( SQLException e )
		{
			log.info( "ERROR: " + e.getMessage() );
			return false;
		}
	}


    @Override public boolean 
    terminate( String lock_id ) 
    {
        try( PreparedStatement ps = connection.prepareStatement(
                " declare "
                + " pragma autonomous_transaction; "
                + " v_pipeline_name varchar2(255) := ?; "
                + " v_lock_id varchar2(255) := ?; " 
                + " begin " 
                + " for v_cur in ( select rowid, rownum "
                + "                  from pipelite_lock "
                + "                 where pipeline_name = v_pipeline_name "
                + "                   and lock_id = v_lock_id "
                + "                   for update skip locked ) " 
                + " loop " 
                + "  update pipelite_lock " 
                + "     set lock_id = null, "
                + "         allocator_name = null "
                + "   where rowid = v_cur.rowid; "
                + "  commit; "
                + "  return; "
                + " end loop; "
                + "  raise_application_error( -20001, 'Lock ' || v_lock_id || ' not found for resource ' || v_pipeline_name ); "
                + " end; "
                 ) )
        {
            ps.setString( 1, this.pipeline_name );
            
            if( null == lock_id )
            {
                ps.setNull( 2, Types.VARCHAR );
            } else
            {
                ps.setString( 2, lock_id );
            }

            ps.execute();
            return true;
        } catch( SQLException e )
        {
            log.info( "ERROR: " + e.getMessage() );
            return false;
        }
    }


    @Override public void 
	purge( Timestamp before_date ) 
	{
		try( PreparedStatement ps = connection.prepareStatement(
				" declare "
				+ " pragma autonomous_transaction; "
				+ " begin "
				+ " update pipelite_lock set lock_id = null, allocator_name = null where pipeline_name = ? and allocator_name = ? and audit_time < ?; "
				+ " commit; "
				+ " end;"
			  ) )
		{
			ps.setString( 1, this.pipeline_name );
			ps.setString( 2, allocator_name );
			ps.setTimestamp( 3, before_date );
			ps.execute();
		} catch( SQLException e )
		{
			log.info( "ERROR: " + e.getMessage() );
		}	
	}



    @Override public boolean
    lock( StageResourceLock rl )
    {
        return tryLock( composeLock( rl ) );
    }



    @Override public boolean
    unlock( StageResourceLock rl )
    {
        return unlock( composeLock( rl ) );
    }



    @Override public boolean
    is_locked( StageResourceLock rl )
    {
        return isBeingHeld( composeLock( rl ) );
    }



    @Override public boolean
    lock( ProcessResourceLock rl )
    {
        return tryLock( composeLock( rl ) );
    }



    @Override public boolean
    unlock( ProcessResourceLock rl )
    {
        return unlock( composeLock( rl ) );
    }



    @Override public boolean
    is_locked( ProcessResourceLock rl )
    {
        return isBeingHeld( composeLock( rl ) );
    }

    
    private String 
    composeLock( ResourceLock rl )
    {
        return rl.getLockId();
    }
    
}
