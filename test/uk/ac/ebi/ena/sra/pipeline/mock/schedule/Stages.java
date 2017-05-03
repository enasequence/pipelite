package uk.ac.ebi.ena.sra.pipeline.mock.schedule;

import uk.ac.ebi.ena.sra.pipeline.launcher.iface.Stage;
import uk.ac.ebi.ena.sra.pipeline.launcher.iface.StageTask;
import uk.ac.ebi.ena.sra.pipeline.mock.stages.FirstStage;
import uk.ac.ebi.ena.sra.pipeline.mock.stages.SecondStage;
import uk.ac.ebi.ena.sra.pipeline.mock.stages.ThirdStage;


public enum 
Stages implements Stage
{
    FIRST( FirstStage.class, 
           "Sample stage", 
           null ),
	SECOND( SecondStage.class, 
            "Second sample stage", 
            FIRST ),
	THIRD( ThirdStage.class, 
           "Third sample stage", 
           SECOND );

	
    Stages( Class<? extends StageTask>  klass,
            String description,
            Stages dependable )            
    {
        this.klass            = klass;
        this.description      = description;
        this.dependable       = dependable;
    }
    
    
    final private Class<? extends StageTask>  klass;
    final private String description;
    final private Stages dependable;
    
    
    public Class<? extends StageTask>
    getTaskClass()
    {
        return klass;
    }
    
    
    public String
    getDescription()
    {
        return description;
    }
    
    
    public Stages
    getDependsOn()
    {
        return dependable;
    }
}
