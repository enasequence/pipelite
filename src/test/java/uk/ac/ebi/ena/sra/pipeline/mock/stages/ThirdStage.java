package uk.ac.ebi.ena.sra.pipeline.mock.stages;

import uk.ac.ebi.ena.sra.pipeline.launcher.iface.StageTask;

public class 
ThirdStage implements StageTask
{

	@Override
	public void 
	init( Object id, 
	      boolean is_forced ) throws Throwable 
	{
		System.out.println( String.format( "Init for %s, id: %s, forced: %s ", 
				                           this.getClass().getSimpleName(), 
				                           id, 
				                           is_forced ) ); 
	}

	@Override
	public void 
	execute() throws Throwable 
	{
		System.out.println( String.format( "Generating failure for %s",
                						   this.getClass().getSimpleName() ) ); 
		try
		{
			Thread.sleep( 5 * 1000 * 60 );
		}catch( InterruptedException io )
		{
			;
		}
		throw new Exception( "Yo, wassup!" );
	}

	@Override
	public void 
	unwind() 
	{
		System.out.println( String.format( "Unwind for %s",
										   this.getClass().getSimpleName() ) ); 
	}

}
