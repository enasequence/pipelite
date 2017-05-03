package uk.ac.ebi.ena.sra.pipeline.launcher;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import uk.ac.ebi.ena.sra.pipeline.configuration.DefaultConfiguration;
import uk.ac.ebi.ena.sra.pipeline.storage.OracleCommons;

public class 
UserParameters 
{
	//TODO: make table, name, value fileds.
	public static class
	UserParameterException extends RuntimeException
	{
		private static final long serialVersionUID = 1L;
		
		public 
		UserParameterException() 
		{
			this( null, null );
		}
		
		
		public 
		UserParameterException( Throwable cause ) 
		{
			this( null, cause );
		}

		
		public 
		UserParameterException( String message, Throwable cause ) 
		{
			super( message, cause );
		}
		
	};
	

	public static class
	UserParameterNotFoundException extends UserParameterException
	{
		private static final long serialVersionUID = 1L;
		
		public 
		UserParameterNotFoundException( String name )
		{
			super( String.format( "Parameter %s not found", name ), null );
		}
	};

	
	private static final String TABLE_NAME        = DefaultConfiguration.currentSet().getUserParameterTableName();
	private static final String ID_COLUMN_NAME    = OracleCommons.PROCESS_COLUMN_NAME;
	private static final String NAME_COLUMN_NAME  = "NAME";
	private static final String VALUE_COLUMN_NAME = "VALUE";
	Logger log = Logger.getLogger( UserParameters.class );
	
	final Connection connection;
	final Object     id;
	
	public 
	UserParameters( Object     id, 
			        Connection connection ) 
	{
		this.id = id;
		this.connection = connection;
	}
	
	
	public String 
	getGlobal( String name ) throws UserParameterException
	{
		return _get( null, name );
	}
	

	public String 
	get( String name ) throws UserParameterException
	{
		return _get( id, name );
	}


	public void 
	deleteGlobal( String name ) throws UserParameterException
	{
		_delete( null, name );
	}
	

	public void 
	delete( String name ) throws UserParameterException
	{
		_delete( id, name );
	}

	
	protected void
	setGlobal( String name, String value ) throws UserParameterException
	{
		_set( null, name, value );
	}
	
	
	public void
	set( String name, String value ) throws UserParameterException
	{
		_set( id, name, value );
	}
	

	private String
	_get( Object id, 
	      String name ) throws UserParameterException
	{
		Statement stmt   = null;
		ResultSet result = null;
		
		try
		{
			stmt   = connection.createStatement();
			result = stmt.executeQuery( String.format( "select %s from %s where %s %s and %s = '%s'",
													   VALUE_COLUMN_NAME,
													   TABLE_NAME,
													   ID_COLUMN_NAME,
													   null == id ? "is null" : String.format( "= '%s'", id ),
													   NAME_COLUMN_NAME,
													   name ) );
			
			while( result.next() )
				return result.getString( 1 );
			throw new UserParameterNotFoundException( name );
		}catch( SQLException sqle )
		{
			throw new UserParameterException( sqle );
			
		} finally
		{
			if( null != result )
				try
				{
					result.close();
					
				}catch( SQLException e )
				{
					e.printStackTrace();
				}
		
				if( null != stmt )
					try
					{
						stmt.close();
						
					}catch( SQLException e )
					{
						e.printStackTrace();
					}
		}
	}

	
	private void
	_set( Object id,
	      String name, 
	      String value ) throws UserParameterException
	{
		Statement stmt = null;
		try
		{
			stmt = connection.createStatement();
			stmt.executeUpdate( String.format( "DECLARE "
											 + " row_num number;"
											 + " PRAGMA AUTONOMOUS_TRANSACTION;" 
											 + " BEGIN" 
											 + "  row_num := 0;"
											 + "  update %s set %s = '%s' where %s %s and %s = '%s' returning 1 into row_num;"
											 + "  if row_num = 0"
											 + "  then"
											 + "   insert into %s( %s, %s, %s ) values( %s, '%s', '%s' ) returning 1 into row_num;"
											 + "  end if;"
											 + "  if row_num = 0"
											 + "  then"
											 + "   raise_application_error( -2000, 'zero rows inserted parameter table' );"
											 + "  end if;"
											 + "  COMMIT;" 
											 + " END;",
											 
											 TABLE_NAME,
											 VALUE_COLUMN_NAME,
											 value,
											 ID_COLUMN_NAME,
											 null == id ? "is null" : String.format( "= '%s'", id ),
									         NAME_COLUMN_NAME,
									         name,
									         TABLE_NAME,
											 ID_COLUMN_NAME,
											 NAME_COLUMN_NAME,
											 VALUE_COLUMN_NAME,
											 null == id ? "is null" : String.format( "'%s'", id ),
										     name,
										     value ) );
			log.info( String.format( "User parameter '%s' for id '%s' inserted/updated to '%s'.", name, id, value ) );
		} catch( SQLException sqle )
		{
			throw new UserParameterException( String.format( "Unable to insert/update row for parameter '%s' for id '%s' updated to '%s'", name, id, value ), sqle );
		} finally 
		{
			if( null != stmt )
				try
				{
					stmt.close();
				} catch( SQLException sqle )
				{
					sqle.printStackTrace();
				}
		}
		
	}
	
	
	private void
	_delete( Object id, 
	         String name ) throws UserParameterException	     
	{
		Statement stmt = null;
		try
		{
			stmt = connection.createStatement();
			stmt.executeUpdate( String.format( "DECLARE "
												+ " row_num number;"
	                                 			+ " PRAGMA AUTONOMOUS_TRANSACTION; "
	                                 			+ "BEGIN"
	                                 			+ " row_num := 0;"
	                                 			+ " delete from %s where %s %s and %s = '%s' returning 1 into row_num;"
	                                 			+ " if row_num = 0 "
	                                 			+ " then "
	                                 			+ "  raise_application_error( -2000, 'unable to delete' );"
	                                 			+ " end if;"
	                                 			+ "COMMIT; "
	                                 			+ "END;", 
	                                 			TABLE_NAME, 
	                                 			ID_COLUMN_NAME,
	                                 			null == id ? "is null" : String.format( "= '%s'", id ),
	                                 			NAME_COLUMN_NAME,
	                                 			name ) );
			log.info( String.format( "User parameter '%s' deleted.", name ) );
		}catch( SQLException sqle )
		{
			throw new UserParameterException( String.format( "Unable to insert row for parameter '%s' for id '%s' updated to '%s'", name, id ), sqle );
			
		} finally 
		{
			if( null != stmt )
				try
				{
					stmt.close();
				} catch( SQLException sqle )
				{
					sqle.printStackTrace();
				}
		}
		
		
	}
	
}
