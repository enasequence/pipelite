# Pipelite. Lightweight pipeline engine.


## Configuration.

There is a configuration file that you need to supply to pipelite each time you run it.


## Properties

Current property set is looking now like:

```
pipelite.process.table.name=[<process> table]
pipelite.process.table.key.column.name=[<process_table_id> field name]
pipelite.stage.table.name=[<stage> table]
pipelite.stage.table.key.column.name=[<stage_table_id> field name]
pipelite.log.table.name=[<log> table]
pipelite.log.table.key.column.name=[<log_table_id> field name]
pipelite.log.sequence.name=[name of sequence for ids of <log_table_id>]
pipelite.process.redo.count=[number of redo times for commit statuses that have canRedo set to true]
pipelite.job.name.prefix=[job prefix which would be part job name for LSF]
pipelite.stages.enum=[full enum name for pipeline stages]
pipelite.commit.status.enum=[full enum name for pipeline commit statuses]
pipelite.default.mail-to=[default mail recipient for error logs]
pipelite.default.lsf-user=[default lsf user to have lsf errors submitted]
pipelite.default.lsf-mem=[memory requested for each lsf job (in megs)]
pipelite.smtp.server=[smtp server for errors message submission]
pipelite.smtp.mail-from=[from field in e-mail]
pipelite.jdbc.driver=[class name for jdbc driver]
pipelite.jdbc.url=[database connect string]
pipelite.jdbc.user=[database user]
pipelite.jdbc.password=[database password]
```

## Enums

Descriptions of pipeline stages (property `pipelite.stages.enum`) and commit statuses (property `pipelite.commit.status.enum`) should be implemented via java enums.
Stages should implement StageDescription interface.

```java
public interface
Stage
{
    public Class<? extends StageTask>   getTaskClass();
    public Stage                        getDependsOn();
    public String                       getDescription();
}
```

Where:
*	`getTaskClass` returns actual class responsible for particular stage
*	`getDependsOn` returns stage from the stage depends on. null if no dependancies. Dependancies used to resolve following dependant stages to clean results in case of failures in current stage.
*	`getDescription` returns textual description for the stage

Note: `stage name (toString())` is used in current version to obtain name for `<stage_table_id>` field. One  needs a good reason to override this method in  their stage enum.
Commit statuses enum should implement `ExecutionResult`. The purpose of it to translate return codes (byte) and `Throwables` to human readable messages and back.

```java
public interface
ExecutionResult
{
    public boolean            canRedo();
    public byte               getExitCode();
    public Class<Throwable>   getCause();
    public String             getMessage();
}
```

*	`canRedo` - returns true if stage can be re-processed having its process_date already set to not null
*	`getExitCode` - returns return code belonging to the commit status. Should be unique for the enum
*	`getCause` - returns throwable cause of commit status. Should be unique for the enum
*	`getMessage` - returns commit status message which would be inserted into process_error field of \<stage> table. Also used for obtaining commit status by given database row.


## Stage tasks
Each stage class should implement `StageTask` interface

```java
public interface
StageTask
{
	public void    init( Object id, boolean is_forced ) throws Throwable;
	public void    execute() throws Throwable;
	public void    unwind();
}
```

Where:
* `init` – initialises stage
*	`execute` – executes stage’s processing code
*	`unwind` – de-initialises stage

Note:
*	`Throwable` thrown by execute method is used to resolve exit code for the stage and then, the exit code is used to resolve commit message for database.
*	Classes implement `StageTask` should have constructors without parameters.


## Database structure

Note: following tables and fields must be presented in database schema. Naming of `<fields>` is up to user but corresponding property values should be set.

### \<process> table

collumn name | data type | nullable | data default
--- | --- | --- | ---
<process_table_id> | VARCHAR2(15 BYTE) | No |
PROCESS_CNT | NUMBER(5,0) | No | 0

### \<stage> table

collumn name | data type
--- | ---
<process_table_id> | VARCHAR2(15 BYTE)
<stage_name> | VARCHAR2(50 BYTE)
PROCESS_DATE | DATE
PROCESS_ERROR | VARCHAR2(4000 BYTE)

### \<log> table

collumn name | data type
--- | ---
<log_table_id> | NUMBER
<process_table_id> | VARCHAR2(15 BYTE)
<stage_name> | VARCHAR(50)
TARGET | VARCHAR2(255 BYTE)
SUCCESS | VARCHAR2(1 BYTE)
LOG_DATE | DATE
LOG_MESSAGE | VARCHAR2(255 BYTE)
LSB_JOBID | NUMBER(10,0)
LSB_HOSTS | VARCHAR2(255 BYTE)


## Classes

Following classes can be launched using command line. Note current implementation of the pipelite needs database structure for particular process instance be  present in data base before launch (e.g, for each process id that user wants to process there should be row in `<process>` table with id and row(s) in `<stage>` table with corresponding prosess ids and names of the stages)

### uk.ac.ebi.ena.sra.pipeline.launcher.Launcher
Launches supervisory program which interact with database and spawns sub-processes  (currently only LSF back-end supported). Does not do any database locking itself, all locking done by calling ProcessLauncher class. Creates file lock to prevent occasional execution of copy.
Command line parameters are:
*	--workers \<number> - number of simultaniously working processes. default value is: 10;
*	--lock \<path> - lock file path, default is: /var/tmp/.launcher.lock
*	--queue \<queue_name> - LSF queue name, default is: research-rh6
*	--mail-to \<list> - comma-separated list of mail addresses, default is: pipelite.default.mail-to value
*	--lsf-user \<user>- user for LSF reports, default is: pipelite.default.lsf-user value
*	--lsf-mem \<number> - memory for single LSF job, default is pipelite.default.lsf-mem value
*	--log-file \<path> - log file, default is /var/tmp/launcher.log

### uk.ac.ebi.ena.sra.pipeline.launcher.ProcessLauncher
Launches execution for process ID, also interacts with data base. Locks corresponding process' and stage' table rows.

*	--stage \<stage_name> - Stage name to execute.
*	--force - Force if error
*	--mail-to \<list>- Comma-separated list of mail addresses, default is: pipelite.default.mail-to value
*	--insert  - inserts process IDs & stage’s names if they are not exists in table. **Warning. Experimental**.

### uk.ac.ebi.ena.sra.pipeline.launcher.StageLauncher
Launches class for supplied stage. It does not lock process and stage tables but current version can insert log records to log table.
*	--id  \<process_table_id> // process instance id to execute
*	--stage \<stage_name> // stage name to execute
*	--force // optional


## Dependencies

Pipelite project needs following libs:
*	mail.jar
*	ojdbc6.jar
*	commons-exec.jar
*	jcommander.jar
*	log4j.jar
