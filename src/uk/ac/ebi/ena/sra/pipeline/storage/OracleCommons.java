package uk.ac.ebi.ena.sra.pipeline.storage;

public interface 
OracleCommons
{
    final static String  PIPELINE_COLUMN_NAME         = "PIPELINE_NAME";    
    final static String  PROCESS_COLUMN_NAME          = "PROCESS_ID";
    final static String  STAGE_NAME_COLUMN_NAME       = "STAGE_NAME";
    
    final static String  ATTEMPT_COLUMN_NAME          = "EXEC_CNT";
    final static String  ENABLED_COLUMN_NAME          = "ENABLED";
    
    final static String  EXEC_ID_SEQUENCE             = "PIPELITE_EXEC_ID_SEQ";
    final static String  EXEC_ID_COLUMN_NAME          = "EXEC_ID";
    final static String  EXEC_START_COLUMN_NAME       = "EXEC_START";
    final static String  EXEC_DATE_COLUMN_NAME        = "EXEC_DATE";
    final static String  EXEC_RESULT_COLUMN_NAME      = "EXEC_RESULT";
    final static String  EXEC_RESULT_TYPE_COLUMN_NAME = "EXEC_RESULT_TYPE";
    final static String  EXEC_STDOUT_COLUMN_NAME      = "EXEC_STDOUT";
    final static String  EXEC_STDERR_COLUMN_NAME      = "EXEC_STDERR";
	final static String  EXEC_CMDLINE_COLUMN_NAME     = "EXEC_CMD_LINE";
    

    final static String  PROCESS_PRIORITY_COLUMN_NAME = "PRIORITY";
    final static String  PROCESS_STATE_COLUMN_NAME    = "STATE";
    final static String  PROCESS_COMMENT_COLUMN_NAME  = "STATE_COMMENT";
    final static String  PROCESS_EXEC_CNT_COLUMN_NAME = "EXEC_CNT";
    
}
