package uk.ac.ebi.ena.sra.pipeline.storage;

import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import uk.ac.ebi.ena.sra.pipeline.launcher.ExecutionInstance;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteState;
import uk.ac.ebi.ena.sra.pipeline.launcher.StageInstance;
import uk.ac.ebi.ena.sra.pipeline.launcher.iface.ExecutionResult.RESULT_TYPE;
import uk.ac.ebi.ena.sra.pipeline.resource.MemoryLocker;
import uk.ac.ebi.ena.sra.pipeline.resource.ProcessResourceLock;
import uk.ac.ebi.ena.sra.pipeline.resource.ResourceLocker;
import uk.ac.ebi.ena.sra.pipeline.resource.StageResourceLock;


public class 
OracleStorage implements OracleCommons, StorageBackend, ResourceLocker
{
    private static final String LOCK_EXPRESSION   = "for update nowait";
    
    @Deprecated private String  log_table_name;
    


    private static final String ENABLED_VALUE = "Y";


    
    private Connection connection;
    private String     pipeline_name;
    private String     stage_table_name;
    private String     process_table_name;
    private MemoryLocker mlocker = new MemoryLocker();
       

    public
    OracleStorage()
    {
    }

    
    @Override public void
    load( PipeliteState state ) throws StorageException
    {
    	load( state, false );
    }
    
    
    @Deprecated public void
    load( PipeliteState state, boolean do_lock ) throws StorageException
    {
        if( null != state.getPipelineName() && !pipeline_name.equals( state.getPipelineName() ) )
        {
            throw new StorageException( "state object pipeline name incorrect" );
        }
            
        try( PreparedStatement ps = connection.prepareStatement( String.format( "select %1$s, %2$s, %3$s, %4$s, %5$s, %7$s "
                                                                              + "  from %6$s "
                                                                              + " where %1$s = ? and %2$s = ? %8$s",
                                                                               /*1*/ PIPELINE_COLUMN_NAME,
                                                                               /*2*/ PROCESS_COLUMN_NAME,
                                                                               /*3*/ PROCESS_PRIORITY_COLUMN_NAME,
                                                                               /*4*/ PROCESS_STATE_COLUMN_NAME,
                                                                               /*5*/ PROCESS_COMMENT_COLUMN_NAME,
                                                                               /*6*/ process_table_name,
                                                                               /*7*/ ATTEMPT_COLUMN_NAME,
                                                                               /*8*/ do_lock ? LOCK_EXPRESSION : "" ) ) )
        {
            ps.setString( 1, pipeline_name );
            ps.setString( 2, state.getProcessId() );
            
            ps.execute();
            try( ResultSet rows = ps.getResultSet() )
            {
                int rownum = 0;
                while( rows.next() )
                {
                    state.setPipelineName( rows.getString( PIPELINE_COLUMN_NAME ) );
                    state.setProcessId( rows.getString( PROCESS_COLUMN_NAME ) );
                    state.setPriority( (int) rows.getLong( PROCESS_PRIORITY_COLUMN_NAME ) );
                    state.setExecCount( (int) rows.getLong( ATTEMPT_COLUMN_NAME ) );
                    String st = rows.getString( PROCESS_STATE_COLUMN_NAME );
                    state.setState( null == state || 0 == st.trim().length() ? PipeliteState.State.ACTIVE
                                                                             : PipeliteState.State.valueOf( st ) );
                    state.setProcessComment( rows.getString( PROCESS_COMMENT_COLUMN_NAME ) );
                    
                    ++ rownum;
                }
                if( 1 != rownum )
                    throw new StorageException( String.format( "Failed to load state for process [%s] of [%s] pipeline", state.getProcessId(), pipeline_name ) );
            }
        } catch( SQLException sqle )
        {
            throw new StorageException( sqle );
        }
    }
    
    
    @Override public void
    save( PipeliteState state ) throws StorageException
    {
        if( null != state.getPipelineName() && !pipeline_name.equals( state.getPipelineName() ) )
        {
            throw new StorageException( "state object pipeline name incorrect" );
        }
            
        if( null == state.getProcessId() || 0 == state.getProcessId().trim().length() )
        {
            throw new StorageException( "state object pipeline process id incorrect" );
        }

        
        PreparedStatement ps = null;
        try
        {
            ps = connection.prepareStatement( String.format(  
                    " merge into %1$s T0 " 
                  + " using " 
                  + " ( " 
                  + "  select ? %2$s, ? %3$s, ? %4$s, ? %5$s, ? %6$s, ? %7$s from dual " 
            + " ) T1 on ( T0.%2$s = T1.%2$s and T0.%3$s = T1.%3$s ) "
            + " when matched then update set  %4$s = T1.%4$s, %5$s = T1.%5$s, %6$s = T1.%6$s, %7$s = T1.%7$s " 
            + " when not matched then insert ( %2$s, %3$s, %4$s, %5$s, %6$s, %7$s ) values ( T1.%2$s, T1.%3$s, T1.%4$s, T1.%5$s, T1.%6$s, T1.%7$s )",
            process_table_name,
            PIPELINE_COLUMN_NAME,
            PROCESS_COLUMN_NAME,
            PROCESS_PRIORITY_COLUMN_NAME,
            PROCESS_STATE_COLUMN_NAME,
            PROCESS_COMMENT_COLUMN_NAME,
            ATTEMPT_COLUMN_NAME ) );
            
                    
            ps.setObject( 1, pipeline_name );
            ps.setObject( 2, state.getProcessId() );                   

            ps.setObject( 3, state.getPriority() );
            ps.setString( 4, state.getState().toString() );
            ps.setObject( 5, state.getProcessComment() );
            ps.setObject( 6, state.getExecCount() );
            
            int rows = ps.executeUpdate();
            if( 1 != rows )
                throw new StorageException( "Can't update exactly 1 row!" );
        } catch( SQLException e )
        {
            throw new StorageException( e );
            
        }finally
        {
            if( null != ps )
            {
                try
                {
                    ps.close();
                }catch( SQLException e )
                {
                    ;
                }
            }
        }
    }
    
    
    @Override public void
    load( ExecutionInstance instance ) throws StorageException
    {
        PreparedStatement ps = null;
        ResultSet rows = null;
        try
        {
            ps = connection.prepareStatement( String.format( "select %s, %s, %s, %s, %s, %s, %s, %s "
                                                           + "  from %s" +
                                                             " where %s = ?", 
                                                             EXEC_ID_COLUMN_NAME,
                                                             EXEC_DATE_COLUMN_NAME,
                                                             EXEC_START_COLUMN_NAME,
                                                             EXEC_RESULT_COLUMN_NAME,
                                                             EXEC_RESULT_TYPE_COLUMN_NAME,
                                                             
                                                             //TODO remove
                                                             EXEC_STDERR_COLUMN_NAME,
                                                             EXEC_STDOUT_COLUMN_NAME,
                                                             EXEC_CMDLINE_COLUMN_NAME,
                                                             
                                                             stage_table_name,
                                                             EXEC_ID_COLUMN_NAME ) );
            
            ps.setObject( 1, instance.getExecutionId() );

            rows = ps.executeQuery();
            int rownum = 0;
            while( rows.next() )
            {
                instance.setFinishTime( rows.getTimestamp( EXEC_DATE_COLUMN_NAME ) );
                instance.setStartTime( rows.getTimestamp( EXEC_START_COLUMN_NAME ) );
                instance.setResult( rows.getString( EXEC_RESULT_COLUMN_NAME ) );
                
                String rtype = rows.getString( EXEC_RESULT_TYPE_COLUMN_NAME );
                instance.setResultType( null == rtype ? null : RESULT_TYPE.valueOf( rtype ) );
                
                //TODO remove
                instance.setStderr( rows.getString( EXEC_STDERR_COLUMN_NAME ) );
                instance.setStdout( rows.getString( EXEC_STDOUT_COLUMN_NAME ) );
                instance.setCmdLine( rows.getString( EXEC_CMDLINE_COLUMN_NAME ) );
                
                ++ rownum;
            }
            if( 1 != rownum )
                throw new StorageException( String.format( "Failed to load execution instance [%s] of [%s] pipeline", instance.getExecutionId(), pipeline_name ) );
        } catch( SQLException e )
        {
            throw new StorageException( e );
            
        }finally
        {
            if( null != rows )
            {
                try
                {
                    rows.close();
                } catch( SQLException e )
                {
                    e.printStackTrace();
                }
            }
            
            if( null != ps )
            {
                try
                {
                    ps.close();
                }catch( SQLException e )
                {
                    ;
                }
            }
        }
    }

    
    @Deprecated public void
    load( StageInstance instance, boolean lock ) throws StorageException
    {
        PreparedStatement ps = null;
        ResultSet rows = null;
                
        try
        {
            ps = connection.prepareStatement( String.format( "select %s, %s, %s from %s" +
                                                             " where %s = ? and %s = ? and %s = ? %s", 
                                                             ATTEMPT_COLUMN_NAME,
                                                             ENABLED_COLUMN_NAME,
                                                             EXEC_ID_COLUMN_NAME,
                                                             stage_table_name,
                                                             PIPELINE_COLUMN_NAME,
                                                             PROCESS_COLUMN_NAME, 
                                                             STAGE_NAME_COLUMN_NAME,
                                                             lock ? LOCK_EXPRESSION : "" ) );
            
            ps.setObject( 1, pipeline_name );
            ps.setObject( 2, instance.getProcessID() );
            ps.setString( 3, instance.getStageName() );
      
            rows = ps.executeQuery();

            int rownum = 0;
            while( rows.next() )
            {
                instance.getExecutionInstance().setExceutionId( rows.getString( EXEC_ID_COLUMN_NAME ) );
                instance.setExecutionCount( (int)rows.getLong( ATTEMPT_COLUMN_NAME ) );
                instance.setEnabled( ENABLED_VALUE.equals( rows.getString( ENABLED_COLUMN_NAME ) ) );
                ++ rownum;
            }
            
            if( 1 != rownum )
                throw new StorageException( String.format( "Failed to load stage [%s] for process [%s] of [%s] pipeline", instance.getStageName(), instance.getProcessID(), pipeline_name ) );
            
            if( null != instance.getExecutionInstance().getExecutionId() )
                load( instance.getExecutionInstance() );
            
        } catch( SQLException e )
        {
            throw new StorageException( e );
            
        }finally
        {
            if( null != rows )
            {
                try
                {
                    rows.close();
                } catch( SQLException e )
                {
                    e.printStackTrace();
                }
            }
            
            if( null != ps )
            {
                try
                {
                    ps.close();
                }catch( SQLException e )
                {
                    ;
                }
            }
        }
        
    }
    
    
    @Override public void
    load( StageInstance instance ) throws StorageException
    {
        load( instance, false );
    }

    
    @Override public void
    save( StageInstance instance ) throws StorageException
    {
    	List<Clob> clob_list = new ArrayList<>(); 
        PreparedStatement ps = null;
        try
        {
            ps = connection.prepareStatement( String.format( 
                    " merge into %14$s T0 "  
                  + " using ( select ? %1$s, ? %2$s, ? %3$s, ? %4$s, ? %5$s, ? %6$s, ? %7$s, ? %8$s, ? %9$s, ? %10$s, ? %11$s, ? %12$s, ? %13$s from dual ) T1 " 
                  + " on ( T0.%1$s = T1.%1$s and T0.%2$s = T1.%2$s and T0.%3$s = T1.%3$s ) "  
                  + " when matched then update set " 
                  + "  %4$s = T1.%4$s, "
                  + "  %5$s = T1.%5$s, "
                  + "  %6$s = T1.%6$s, "
                  + "  %7$s = T1.%7$s, "
                  + "  %8$s = T1.%8$s, "
                  + "  %9$s = T1.%9$s, "
                  + "  %10$s = T1.%10$s, "
                  + "  %11$s = T1.%11$s, "
                  + "  %12$s = T1.%12$s, "
                  + "  %13$s = T1.%13$s "
                  + " when not matched then insert( %1$s, %2$s, %3$s, %4$s, %5$s, %6$s, %7$s, %8$s, %9$s, %10$s, %11$s, %12$s, %13$s ) "
                  + "                       values( T1.%1$s, T1.%2$s, T1.%3$s, T1.%4$s, T1.%5$s, T1.%6$s, T1.%7$s, T1.%8$s, T1.%9$s, T1.%10$s, T1.%11$s, T1.%12$s, T1.%13$s ) ",
/* 1 */           PIPELINE_COLUMN_NAME,
/* 2 */           PROCESS_COLUMN_NAME, 
/* 3 */           STAGE_NAME_COLUMN_NAME,
/* 4 */           ATTEMPT_COLUMN_NAME,
/* 5 */           EXEC_ID_COLUMN_NAME,
/* 6 */           ENABLED_COLUMN_NAME,
/* 7 */           EXEC_START_COLUMN_NAME,
/* 8 */           EXEC_DATE_COLUMN_NAME,
/* 9 */           EXEC_RESULT_TYPE_COLUMN_NAME,
/* 10 */          EXEC_RESULT_COLUMN_NAME,
/* 11 */          EXEC_STDOUT_COLUMN_NAME,
/* 12 */          EXEC_STDERR_COLUMN_NAME,
/* 13 */          EXEC_CMDLINE_COLUMN_NAME,
/* 14 */          getTableName() ) );
 
            ps.setObject( 1, pipeline_name );
            ps.setObject( 2, instance.getProcessID() );
            ps.setString( 3, instance.getStageName() );

            ps.setObject( 4, instance.getExecutionCount() );
            ps.setObject( 5, null == instance.getExecutionInstance() ? null : instance.getExecutionInstance().getExecutionId() );
            ps.setObject( 6, instance.isEnabled() ? "Y" : "N" );
            
            ps.setObject( 7, null == instance.getExecutionInstance() ? null : instance.getExecutionInstance().getStart() );
            ps.setObject( 8, null == instance.getExecutionInstance() ? null : instance.getExecutionInstance().getFinish() );
            ps.setString( 9, null == instance.getExecutionInstance() ? null : null == instance.getExecutionInstance().getResultType() ? null : instance.getExecutionInstance().getResultType().toString() );
      
            ps.setObject( 10, null == instance.getExecutionInstance() ? null : instance.getExecutionInstance().getResult() );
            
            ps.setObject( 11, makeClob( connection, null == instance.getExecutionInstance() ? null : instance.getExecutionInstance().getStdout(), clob_list ) );
            ps.setObject( 12, makeClob( connection, null == instance.getExecutionInstance() ? null : instance.getExecutionInstance().getStderr(), clob_list ) );
            ps.setObject( 13, makeClob( connection, null == instance.getExecutionInstance() ? null : instance.getExecutionInstance().getCmdLine(), clob_list ) );
            
            int rows = ps.executeUpdate();
            if( 1 != rows )
                throw new StorageException( "Can't update exactly 1 row!" );
/*            
            if( null != instance.getExecutionInstance().getExecutionId() )
                save( instance.getExecutionInstance() );
*/
        } catch( SQLException e )
        {
            throw new StorageException( e );
            
        }finally
        {
        	clob_list.forEach( e -> {
				try {
					e.free();
				} catch( SQLException e1 ) 
				{
					throw new RuntimeException( e1 );
				}
			} );
        	
            if( null != ps )
            {
                try
                {
                    ps.close();
                }catch( SQLException e )
                {
                    ;
                }
            }
        }
    }
 
    
    private Clob
    makeClob( Connection connection, String value, List<Clob> list ) throws SQLException
    {
    	Clob clob = connection.createClob();
    	clob.setString( 1, value );
    	list.add( clob );
    	return clob;
    }
    
    
    //TODO add exec_id
    @Override public void 
    save( ProcessLogBean bean ) throws StorageException
    {
        PreparedStatement ps = null;
        try
        {
            ps = connection.prepareStatement( String.format( "DECLARE "
                                                           + " PRAGMA AUTONOMOUS_TRANSACTION; "
                                                           + "BEGIN"
                                                           + " insert into %s ( %s, %s, %s, %s, LOG_DATE, MESSAGE, EXCEPTION, JOBID, HOSTS ) "
                                                           + " values ( ?, ?, ?, ?, sysdate,  ?, ?, ?, ? );"
                                                           + " IF 1 <> sql%%rowcount THEN "
                                                           + "  ROLLBACK;"
                                                           + "  raise_application_error( -20000, 'Insert into %s failed' ); "
                                                           + " END IF;"
                                                           + "COMMIT; "
                                                           + "END;", 
                                                           log_table_name,
                                                           PIPELINE_COLUMN_NAME,
                                                           PROCESS_COLUMN_NAME,
                                                           STAGE_NAME_COLUMN_NAME,
                                                           EXEC_ID_COLUMN_NAME,
                                                           log_table_name ) );
            ps.setString( 1, bean.getPipelineName() );
            ps.setString( 2, bean.getProcessID() );
            ps.setString( 3, bean.getStage() );
            ps.setObject( 4, bean.getExecutionId() );
            ps.setString( 5, bean.getMessage() );
            ps.setString( 6, null != bean.getExceptionText() && bean.getExceptionText().length() > 3000 ? bean.getExceptionText().substring( 0, 3000 ) : bean.getExceptionText() );
            ps.setObject( 7, bean.getLSFJobID() );
            ps.setString( 8, bean.getLSFHost() );
            
            int rows = ps.executeUpdate();
            if( 1 != rows )
                throw new StorageException( String.format( "Unable to insert into %s", log_table_name ) );
            
        } catch (SQLException e) 
        {
            throw new StorageException( e );
            
        } catch( NoSuchFieldException e )
        {
            throw new StorageException( e );
            
        } finally
        {
            if( null != ps )
            {
                try
                {
                    ps.close();
                }catch( SQLException e )
                {
                    ;
                }
            }
        }
    }


    @Override
    public void 
    flush() throws StorageException
    {
        try
        {
            connection.commit();
        } catch( SQLException e )
        {
            throw new StorageException( e );
        }
    }


    @Override
    public void 
    close() throws StorageException
    {
        try
        {
            connection.close();
        } catch( SQLException e )
        {
            throw new StorageException( e );
        }
    }


    public Connection
    getConnection()
    {
        return connection;
    }


    public void
    setConnection( Connection connection )
    {
        this.connection = connection;
    }


    public String
    getPipelineName()
    {
        return pipeline_name;
    }


    public void
    setPipelineName( String pipeline_name )
    {
        this.pipeline_name = pipeline_name;
    }


    public String
    getTableName()
    {
        return stage_table_name;
    }


    public void
    setStageTableName( String stage_table_name )
    {
        this.stage_table_name = stage_table_name;
    }


    @Deprecated public String
    getLogTableName()
    {
        return log_table_name;
    }


    @Deprecated public void
    setLogTableName( String log_table_name )
    {
        this.log_table_name = log_table_name;
    }


    public String
    getStateTableName()
    {
        return process_table_name;
    }


    public void
    setProcessTableName( String state_table_name )
    {
        this.process_table_name = state_table_name;
    }


    @Override public void
    save( ExecutionInstance instance ) throws StorageException
    {
    	List<Clob> clob_list = new ArrayList<>();
        PreparedStatement ps = null;
        try
        {
            ps = connection.prepareStatement( String.format( "update %s "
                                                           + "   set %s = ?, %s = ?, %s = ?, "
                                                           + "       %s = ?, %s = ?, %s = ?, %s = ? "
                                                           + " where %s = ?", 
                                                             stage_table_name,
                                                             EXEC_START_COLUMN_NAME,
                                                             EXEC_DATE_COLUMN_NAME,
                                                             EXEC_RESULT_TYPE_COLUMN_NAME,
                                                             EXEC_RESULT_COLUMN_NAME,
                                                             EXEC_STDERR_COLUMN_NAME,
                                                             EXEC_STDOUT_COLUMN_NAME,
                                                             EXEC_CMDLINE_COLUMN_NAME,
                                                             EXEC_ID_COLUMN_NAME ) );
            
            ps.setObject( 1, instance.getStart() );
            ps.setObject( 2, instance.getFinish() );
            ps.setString( 3, null == instance.getResultType() ? null : instance.getResultType().toString() );
            
            ps.setObject( 4, instance.getResult() );
            
            ps.setObject( 5, makeClob( connection, instance.getStderr(), clob_list ) );
            ps.setObject( 6, makeClob( connection, instance.getStdout(), clob_list ) );
            ps.setObject( 7, makeClob( connection, instance.getCmdLine(), clob_list ) );
            
            ps.setString( 8, instance.getExecutionId() );
      
            int rows = ps.executeUpdate();
            if( 1 != rows )
                throw new StorageException( "Can't update exactly 1 row!" );
            
        } catch( SQLException e )
        {
            throw new StorageException( e );
            
        }finally
        {
        	clob_list.forEach( e -> {
				try 
				{
					e.free();
				}catch( SQLException e1 ) 
				{
					throw new RuntimeException( e1 );
				}
			} );
        	
        	
            if( null != ps )
            {
                try
                {
                    ps.close();
                }catch( SQLException e )
                {
                    ;
                }
            }
        }

    }


    //TODO specify contract
    @Override public boolean
    lock( StageResourceLock rl )
    {
        StageInstance si = new StageInstance();
        si.setPipelineName( pipeline_name );
        si.setStageName( rl.getLockId() );
        si.setProcessID( rl.getLockOwner() );
        try
        {
            load( si, true );
            return mlocker.lock( rl );
        } catch( StorageException e )
        {
            return false;
        }
    }


    @Override public boolean
    unlock( StageResourceLock rl )
    {
        return mlocker.unlock( rl );
    }


    @Override public boolean
    is_locked( StageResourceLock rl )
    {
        return mlocker.is_locked( rl );
    }

    
    //TODO specify contract
    @Override public boolean
    lock( ProcessResourceLock rl )
    {
        PipeliteState pi = new PipeliteState();
        pi.setPipelineName( pipeline_name );
        pi.setProcessId( rl.getLockId() );
        try
        {
            load( pi, true );
            return mlocker.lock( rl );
        } catch( StorageException e )
        {
            return false;
        }
    }


    @Override public boolean
    unlock( ProcessResourceLock rl )
    {
        return mlocker.unlock( rl );
    }


    @Override public boolean
    is_locked( ProcessResourceLock rl )
    {
        return mlocker.is_locked( rl );
    }

    
    @Override public String
    getExecutionId() throws StorageException
    {
        
        try( PreparedStatement ps = connection.prepareStatement( String.format( "select %s.nextVal from dual", EXEC_ID_SEQUENCE ) ) )
        {
            try( ResultSet rs = ps.executeQuery() )
            {
                while( rs.next() )
                    return rs.getString( 1 );
            }
        } catch( SQLException e )
        {
            throw new StorageException( e );
        }
        return null;
    }
}
