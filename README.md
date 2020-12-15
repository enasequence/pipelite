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

-- TODO: remove 'SKIPPED', 'TRANSIENT_ERROR', 'PERMANENT_ERROR'
alter table pipelite_stage add constraint CK_PIPELITE_STAGE_3 check
(
  exec_result_type is null or exec_result_type in ( 'ACTIVE', 'SUCCESS', 'SKIPPED', 'TRANSIENT_ERROR', 'PERMANENT_ERROR', 'ERROR' )
);

alter table pipelite_process drop constraint CK_PIPELITE_PROCESS_1;

-- TODO: remove 'INACTIVE'
alter table pipelite_process add constraint CK_PIPELITE_PROCESS_1 check
(
    state in ( 'PENDING', 'ACTIVE', 'INACTIVE', 'COMPLETED', 'CANCELLED', 'FAILED' )
);

create table pipelite_schedule
(
    pipeline_name varchar2(64) not null,
    scheduler_name varchar2(256) not null,
	cron varchar2(256) not null,
	active char(1),
	description varchar2(256),
	exec_start date,
	exec_date date,
    exec_cnt number(10,0) default 0 not null,
    process_id varchar2(64),
	audit_time date default sysdate not null,
	audit_user varchar2(30) default user not null,
	audit_osuser varchar2(30) default SYS_CONTEXT( 'USERENV', 'OS_USER' ) not null,
	constraint pk_pipelite_schedule primary key (pipeline_name)
)
tablespace era_tab;

create table pipelite_schedule_audit
(
    pipeline_name varchar2(64),
    scheduler_name varchar2(256),
	cron varchar2(256),
	active char(1),
	description varchar2(256),
	exec_start date,
	exec_date date,
    exec_cnt number(10,0),
    process_id varchar2(64),
    audit_time date not null,
    audit_user varchar2(30) not null,
    audit_osuser varchar2(30),
    to_audit_time date default sysdate not null,
    to_audit_user varchar2(30) default user not null,
    to_audit_osuser varchar2(30) default SYS_CONTEXT('USERENV','OS_USER'),
    audit_stmt char(1 byte) not null
)
tablespace era_tab;

create or replace trigger pipelite_schedule_audit
before insert or update or delete on pipelite_schedule
for each row
declare
    audit_stmt varchar2(1);
begin
    if ( updating or inserting ) then
      :new.audit_time := sysdate;
      :new.audit_user := user;
      :new.audit_osuser := SYS_CONTEXT('USERENV','OS_USER');
    end if;

    if updating then
      audit_stmt := 'U';
    elsif deleting then
      audit_stmt := 'D';
    end if;

    if ( updating or deleting ) then
      insert into pipelite_schedule_audit (
        pipeline_name,
        scheduler_name,
    	cron,
    	active,
    	description,
	    exec_start,
     	exec_date,
        exec_cnt,
        process_id,
        audit_time,
        audit_user,
        audit_osuser,
        audit_stmt
      )
      values
      (
        :old.pipeline_name,
        :old.scheduler_name,
    	:old.cron,
    	:old.active,
    	:old.description,
	    :old.exec_start,
        :old.exec_date,
        :old.exec_cnt,
        :old.process_id,
        :old.audit_time,
        :old.audit_user,
        :old.audit_osuser,
        audit_stmt
      );
    end if;
end;
/

create table pipelite_process_lock
(
    launcher_id number(15,0) not null,
    pipeline_name varchar2(64) not null,
    process_id varchar2(256) not null,
	audit_time date default sysdate not null,
	audit_user varchar2(30) default user not null,
	audit_osuser varchar2(30) default SYS_CONTEXT( 'USERENV', 'OS_USER' ) not null,
	constraint pk_pipelite_process_lock primary key (process_id, pipeline_name)
)
tablespace era_tab;

create table pipelite_launcher_lock
(
    launcher_id number(15,0) not null,
    launcher_name varchar2(256) not null,
    launcher_type varchar2(64) not null,
    host varchar2(256) not null,
    port number(5) not null,
    context_path varchar2(256) not null,
    expiry timestamp not null,
	audit_time date default sysdate not null,
	audit_user varchar2(30) default user not null,
	audit_osuser varchar2(30) default SYS_CONTEXT( 'USERENV', 'OS_USER' ) not null,
	constraint pk_pipelite_launcher_lock primary key (launcher_id),
	constraint uk_pipelite_launcher_lock unique (launcher_name)
)
tablespace era_tab;

create sequence pipelite_launcher_lock_seq
increment by 1
start with 1;

/*
create or replace trigger pipelite_launcher_lock
before insert on pipelite_launcher_lock
for each row
begin
   :new.launcher_id := pipelite_launcher_lock_seq.nextval;
end;
/
*/

create public synonym pipelite_process_lock for pipelite_process_lock;
create public synonym pipelite_launcher_lock for pipelite_launcher_lock;
