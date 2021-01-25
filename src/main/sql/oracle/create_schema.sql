-- define table_tablespace = 'TODO';
-- define index_tablespace = 'TODO';
-- define table_tablespace = 'era_tab';
-- define index_tablespace = 'era_ind';

create sequence pipelite2_launcher_lock_seq
    increment by 1
    start with 1;

create table pipelite2_launcher_lock
(
    launcher_id   number(15,0) not null,
    launcher_name varchar2(256) not null,
    launcher_type varchar2(64) not null,
    host          varchar2(256) not null,
    port          number(5,0) not null,
    context_path  varchar2(256) not null,
    expiry        timestamp            not null,
    audit_time    date default sysdate not null
) tablespace &table_tablespace;

-- @formatter:off

create unique index pk_pipelite2_launcher_lock on pipelite2_launcher_lock (launcher_id)
tablespace &index_tablespace;

alter table pipelite2_launcher_lock add constraint pk_pipelite2_launcher_lock
primary key (launcher_id) using index pk_pipelite2_launcher_lock
;

create unique index uk_pipelite2_launcher_lock on pipelite2_launcher_lock (launcher_name)
tablespace &index_tablespace;

alter table pipelite2_launcher_lock add constraint uk_pipelite2_launcher_lock
unique (launcher_name) using index uk_pipelite2_launcher_lock
;

-- @formatter:on

create table pipelite2_process
(
    pipeline_name varchar2(64) not null,
    process_id    varchar2(64) not null,
    priority      number (1,0) not null,
    state         varchar2(15) not null,
    exec_cnt      number (5,0) not null,
    exec_start    date,
    exec_end      date,
    state_comment varchar2(4000),
    audit_time    date default sysdate not null
) tablespace &table_tablespace;

-- @formatter:off

create unique index pk_pipelite2_process on pipelite2_process (process_id, pipeline_name)
tablespace &index_tablespace;

alter table pipelite2_process add constraint pk_pipelite2_process
primary key (process_id, pipeline_name) using index pk_pipelite2_process
;

alter table pipelite2_process add constraint ck_pipelite2_process_state
check ( state in ('PENDING', 'ACTIVE', 'FAILED', 'COMPLETED', 'CANCELLED') )
;

create index i_pipelite2_process_state on pipelite2_process (state)
tablespace &index_tablespace;

-- @formatter:on

create table pipelite2_process_audit
(
    pipeline_name varchar2(64),
    process_id    varchar2(64),
    priority      number (1,0),
    state         varchar2(15),
    exec_cnt      number (5,0),
    exec_start    date,
    exec_end      date,
    state_comment varchar2(4000),
    audit_time    date,
    to_audit_time date,
    audit_stmt    char(1)
) tablespace &table_tablespace;

-- @formatter:off

create or replace trigger pipelite2_process_audit
before insert or update or delete
on pipelite2_process
for each row
declare
    audit_stmt varchar2(1);
begin
    if updating or inserting then
        :new.audit_time := sysdate;
    end if;

    if updating then
        audit_stmt := 'U';
    elsif deleting then
        audit_stmt := 'D';
    end if;

    if updating or deleting
    then
        insert into pipelite2_process_audit (
            pipeline_name,
            process_id,
            priority,
            state,
            exec_start,
            exec_end,
            exec_cnt,
            state_comment,
            audit_time,
            audit_stmt
        )
        values
        (
            :old.pipeline_name,
            :old.process_id,
            :old.priority,
            :old.state,
            :old.exec_start,
            :old.exec_end,
            :old.exec_cnt,
            :old.state_comment,
            :old.audit_time,
            audit_stmt
        );
    end if;
end;
/

-- @formatter:on

create table pipelite2_process_lock
(
    launcher_id   number(15,0) not null,
    pipeline_name varchar2(64) not null,
    process_id    varchar2(256) not null,
    audit_time    date default sysdate not null
) tablespace &table_tablespace;

-- @formatter:off

create unique index pk_pipelite2_process_lock on pipelite2_process_lock (process_id, pipeline_name)
tablespace &index_tablespace;

alter table pipelite2_process_lock add constraint pk_pipelite2_process_lock
primary key (process_id, pipeline_name) using index pk_pipelite2_process_lock
;

-- @formatter:on

create table pipelite2_schedule
(
    pipeline_name    varchar2(64) not null,
    scheduler_name   varchar2(256) not null,
    cron             varchar2(256) not null,
    description      varchar2(256),
    active           char(1) default 'Y' not null,
    process_id       varchar2(64),
    exec_start       date,
    exec_end         date,
    exec_cnt         number(10,0) default 0 not null,
    exec_next        date,
    last_completed   date,
    last_failed      date,
    streak_completed number(10,0) default 0 not null,
    streak_failed    number(10,0) default 0 not null,
    audit_time       date    default sysdate not null
) tablespace &table_tablespace;

-- @formatter:off

create unique index pk_pipelite2_schedule on pipelite2_schedule (pipeline_name)
tablespace &index_tablespace;

alter table pipelite2_schedule add constraint pk_pipelite2_schedule
primary key (pipeline_name) using index pk_pipelite2_schedule
;

-- @formatter:on

create table pipelite2_schedule_audit
(
    pipeline_name    varchar2(64),
    scheduler_name   varchar2(256),
    cron             varchar2(256),
    description      varchar2(256),
    active           char(1),
    process_id       varchar2(64),
    exec_start       date,
    exec_end         date,
    exec_cnt         number(10,0),
    exec_next        date,
    last_completed   date,
    last_failed      date,
    streak_completed number(10,0),
    streak_failed    number(10,0),
    audit_time       date,
    to_audit_time    date,
    audit_stmt       char(1)
) tablespace &table_tablespace;

-- @formatter:off

create or replace trigger pipelite2_schedule_audit
before insert or update or delete on pipelite2_schedule
for each row
declare
    audit_stmt varchar2(1);
begin
    if updating or inserting then
        :new.audit_time := sysdate;
    end if;

    if updating then
        audit_stmt := 'U';
    elsif deleting then
        audit_stmt := 'D';
    end if;

    if updating or deleting then
        insert into pipelite2_schedule_audit (
            pipeline_name,
            scheduler_name,
            cron,
            description,
            active,
            process_id,
            exec_start,
            exec_end,
            exec_next,
            exec_cnt,
            last_completed,
            last_failed,
            streak_completed,
            streak_failed,
            audit_time,
            audit_stmt
        )
        values
        (
            :old.pipeline_name,
            :old.scheduler_name,
            :old.cron,
            :old.description,
            :old.active,
            :old.process_id,
            :old.exec_start,
            :old.exec_end,
            :old.exec_next,
            :old.exec_cnt,
            :old.last_completed,
            :old.last_failed,
            :old.streak_completed,
            :old.streak_failed,
            :old.audit_time,
            audit_stmt
        );
    end if;
end;
/

-- @formatter:on

create table pipelite2_stage
(
    pipeline_name      varchar2(64) not null,
    process_id         varchar2(64) not null,
    stage_name         varchar2(256) not null,
    state              varchar2(15) not null,
    exec_start         date,
    exec_end           date,
    exec_cnt           number(5,0) default 0 not null,
    exec_name          varchar2(256),
    exec_data          clob,
    exec_params        clob,
    exec_result_params clob,
    audit_time         date default sysdate not null
) tablespace &table_tablespace;

-- @formatter:off

create unique index pk_pipelite2_stage on pipelite2_stage (process_id, stage_name, pipeline_name)
tablespace &index_tablespace;

alter table pipelite2_stage add constraint pk_pipelite2_stage
primary key (process_id, stage_name, pipeline_name) using index pk_pipelite2_stage
;

alter table pipelite2_stage add constraint ck_pipelite2_stage_result
check ( state in ('PENDING', 'ACTIVE', 'SUCCESS', 'ERROR') )
;

-- @formatter:on

create table pipelite2_stage_audit
(
    pipeline_name      varchar2(64),
    process_id         varchar2(64),
    stage_name         varchar2(256),
    state              varchar2(15),
    exec_start         date,
    exec_end           date,
    exec_cnt           number(5,0),
    exec_name          varchar2(256),
    exec_data          clob,
    exec_params        clob,
    exec_result_params clob,
    audit_time         date,
    to_audit_time      date,
    audit_stmt         char(1)
) tablespace &table_tablespace;

-- @formatter:off

create or replace trigger pipelite2_stage_audit
before insert or update or delete on pipelite2_stage
for each row
declare
    audit_stmt varchar2(1);
begin
    if updating or inserting then
        :new.audit_time := sysdate;
    end if;

    if updating then
        audit_stmt := 'U';
    elsif deleting then
        audit_stmt := 'D';
    end if;

    if updating or deleting then
      insert into pipelite2_stage_audit (
        pipeline_name,
        process_id,
        stage_name,
        state,
        exec_start,
        exec_end,
        exec_cnt,
        exec_name,
        exec_data,
        exec_params,
        exec_result_params,
        audit_time,
        audit_stmt
      )
      values
      (
        :old.pipeline_name,
        :old.process_id,
        :old.stage_name,
        :old.state,
        :old.exec_start,
        :old.exec_end,
        :old.exec_cnt,
        :old.exec_name,
        :old.exec_data,
        :old.exec_params,
        :old.exec_result_params,
        :old.audit_time,
        audit_stmt
      );
end if;
end;
/

-- @formatter:on

create table pipelite2_stage_log
(
    pipeline_name varchar2(64) not null,
    process_id    varchar2(64) not null,
    stage_name    varchar2(255) not null,
    stage_log     clob,
    audit_time    date default sysdate not null
) tablespace &table_tablespace;

-- @formatter:off

create unique index pk_pipelite2_stage_log on pipelite2_stage_log (process_id, stage_name, pipeline_name)
tablespace &index_tablespace;

alter table pipelite2_stage_log add constraint pk_pipelite2_stage_log
primary key (process_id, stage_name, pipeline_name) using index pk_pipelite2_stage_log
;

-- @formatter:on