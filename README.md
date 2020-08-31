TODO: ERA schema changes

alter table pipelite_process modify (
  process_id varchar2(64)
);

alter table pipelite_stage modify (
  process_id varchar2(64)
);

alter table pipelite_stage add (
  exec_name varchar2(4000),
  exec_data clob
);

alter table pipelite_stage drop constraint CK_PIPELITE_STAGE_3;

alter table pipelite_stage add constraint CK_PIPELITE_STAGE_3 check
(
  exec_result_type is null or exec_result_type in ( 'NEW', 'ACTIVE', 'SUCCESS', 'SKIPPED', 'TRANSIENT_ERROR', 'PERMANENT_ERROR', 'INTERNAL_ERROR' )
);


alter table pipelite_process drop constraint CK_PIPELITE_PROCESS_1;

alter table pipelite_process add constraint CK_PIPELITE_PROCESS_1 check
(
    state in ( 'NEW', 'ACTIVE', 'INACTIVE', 'COMPLETED', 'CANCELLED', 'FAILED' )
);
