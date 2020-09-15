# TODO: ERA schema changes

alter table pipelite_process modify (
  process_id varchar2(64)
);

alter table pipelite_stage modify (
  process_id varchar2(64)
);

alter table pipelite_stage add (
  EXEC_PARAMS clob,
  EXEC_RESULT_PARAMS clob
);

alter table pipelite_stage add (
  exec_name varchar2(4000),
  exec_data clob
);

alter table pipelite_stage drop constraint CK_PIPELITE_STAGE_3;

alter table pipelite_stage add constraint CK_PIPELITE_STAGE_3 check
(
  exec_result_type is null or exec_result_type in ( 'NEW', 'ACTIVE', 'SUCCESS', 'SKIPPED', 'TRANSIENT_ERROR', 'PERMANENT_ERROR', 'ERROR' )
);

alter table pipelite_process drop constraint CK_PIPELITE_PROCESS_1;

alter table pipelite_process add constraint CK_PIPELITE_PROCESS_1 check
(
    state in ( 'NEW', 'ACTIVE', 'INACTIVE', 'COMPLETED', 'CANCELLED', 'FAILED' )
);

# TODO: Singularity

It is possible to run docker images using singularity on YODA/NOAH/SRA cluster:

bsub "singularity run docker://enasequence/webin-cli"

We could have a contract where we bundle sra-stages into a docker image:
....
ENTRYPOINT "java", "-jar", "XXX.jar"]

where the main expects <pipeline name> <process id> <stage name>

and we call it:

bsub "singularity run docker://enasequence/webin-cli <process name> <process id> <stage name>"
