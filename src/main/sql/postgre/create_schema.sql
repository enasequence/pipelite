-- define table_tablespace = 'TODO';
-- define index_tablespace = 'TODO';
-- define table_tablespace = 'enappro_tab';
-- define index_tablespace = 'enappro_tab';

create table pipelite2_service_lock
(
    lock_id      varchar(36) not null primary key,
    service_name varchar(256) not null,
    host         varchar(256) not null,
    port         numeric(5,0) not null,
    context_path varchar(256) not null,
    expiry       timestamp with time zone not null,
    audit_time   timestamp with time zone default current_timestamp not null
) tablespace &table_tablespace;

-- @formatter:off

alter table pipelite2_service_lock add constraint uk_pipelite2_service_lock
unique (service_name);

-- @formatter:on

create table pipelite2_process
(
    pipeline_name varchar(256) not null,
    process_id    varchar(256) not null,
    priority      numeric (1,0) default 5 not null,
    state         varchar(15) default 'PENDING' not null,
    created       timestamp with time zone default current_timestamp not null,
    exec_cnt      numeric (5,0) default 0 not null,
    exec_start    timestamp with time zone,
    exec_end      timestamp with time zone,
    state_comment varchar(4000),
    audit_time    timestamp with time zone default current_timestamp not null,
    primary key (process_id, pipeline_name)
) tablespace &table_tablespace;

-- @formatter:off

alter table pipelite2_process add constraint ck_pipelite2_process_state
check ( state in ('PENDING', 'ACTIVE', 'FAILED', 'COMPLETED', 'CANCELLED') )
;

create index i_pipelite2_process_state on pipelite2_process (state)
tablespace &index_tablespace;

create index i_pipelite2_process_name on pipelite2_process (pipeline_name)
tablespace &index_tablespace;

-- @formatter:on

create table pipelite2_process_audit
(
    pipeline_name varchar(256),
    process_id    varchar(256),
    priority      numeric (1,0),
    state         varchar(15),
    created       timestamp with time zone,
    exec_cnt      numeric (5,0),
    exec_start    timestamp with time zone,
    exec_end      timestamp with time zone,
    state_comment varchar(4000),
    audit_time    timestamp with time zone,
    audit_stmt    char(1)
) tablespace &table_tablespace;

-- @formatter:off

CREATE FUNCTION pipelite2_process_audit()
    RETURNS TRIGGER
    LANGUAGE PLPGSQL
AS $$
    declare
        audit_stmt varchar(1);
    begin
        if tg_op in ('INSERT', 'UPDATE') then
            new.audit_time := current_timestamp;
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
                created,
                exec_start,
                exec_end,
                exec_cnt,
                state_comment,
                audit_time,
                audit_stmt
            )
            values
            (
                old.pipeline_name,
                old.process_id,
                old.priority,
                old.state,
                old.created,
                old.exec_start,
                old.exec_end,
                old.exec_cnt,
                old.state_comment,
                old.audit_time,
                audit_stmt
            );
        end if;
    end;
$$;

create or replace trigger pipelite2_process_audit
before insert or update or delete
on pipelite2_process
for each row
execute procedure pipelite2_process_audit();

-- @formatter:on

create table pipelite2_process_lock
(
    service_name  varchar(256) not null,
    pipeline_name varchar(256) not null,
    process_id    varchar(256) not null,
    audit_time    timestamp with time zone default current_timestamp not null,
    primary key(process_id, pipeline_name)
) tablespace &table_tablespace;

-- @formatter:off

-- @formatter:on

create table pipelite2_schedule
(
    pipeline_name    varchar(256) not null primary key,
    service_name     varchar(256) not null,
    cron             varchar(256) not null,
    description      varchar(256),
    process_id       varchar(256),
    exec_start       timestamp with time zone,
    exec_end         timestamp with time zone,
    exec_cnt         numeric(10,0) default 0 not null,
    exec_next        timestamp with time zone,
    last_completed   timestamp with time zone,
    last_failed      timestamp with time zone,
    streak_completed numeric(10,0) default 0 not null,
    streak_failed    numeric(10,0) default 0 not null,
    audit_time       timestamp with time zone default current_timestamp not null
) tablespace &table_tablespace;

-- @formatter:off

-- @formatter:on

create table pipelite2_schedule_audit
(
    pipeline_name    varchar(256),
    service_name     varchar(256),
    cron             varchar(256),
    description      varchar(256),
    process_id       varchar(256),
    exec_start       timestamp with time zone,
    exec_end         timestamp with time zone,
    exec_cnt         numeric(10,0),
    exec_next        timestamp with time zone,
    last_completed   timestamp with time zone,
    last_failed      timestamp with time zone,
    streak_completed numeric(10,0),
    streak_failed    numeric(10,0),
    audit_time       timestamp with time zone,
    audit_stmt       char(1)
) tablespace &table_tablespace;

-- @formatter:off

CREATE FUNCTION pipelite2_schedule_audit()
    RETURNS TRIGGER
    LANGUAGE PLPGSQL
AS $$
declare
audit_stmt varchar(1);
begin
    if tg_op in ('INSERT', 'UPDATE') then
        new.audit_time := current_timestamp;
    end if;

    if updating then
        audit_stmt := 'U';
    elsif deleting then
        audit_stmt := 'D';
    end if;

    if updating or deleting then
        insert into pipelite2_schedule_audit (
            pipeline_name,
            service_name,
            cron,
            description,
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
            old.pipeline_name,
            old.service_name,
            old.cron,
            old.description,
            old.process_id,
            old.exec_start,
            old.exec_end,
            old.exec_next,
            old.exec_cnt,
            old.last_completed,
            old.last_failed,
            old.streak_completed,
            old.streak_failed,
            old.audit_time,
            audit_stmt
        );
    end if;
end;
$$;

create trigger pipelite2_schedule_audit
before insert or update or delete
on pipelite2_schedule
for each row
execute procedure pipelite2_schedule_audit();

-- @formatter:on

create table pipelite2_stage
(
    pipeline_name      varchar(256) not null,
    process_id         varchar(256) not null,
    stage_name         varchar(256) not null,
    state              varchar(15) not null,
    error_type         varchar(64),
    exec_start         timestamp with time zone,
    exec_end           timestamp with time zone,
    exec_cnt           numeric(5,0) default 0 not null,
    exec_name          varchar(256),
    exec_data          text,
    exec_params        text,
    exec_result_params text,
    exit_code          numeric(5,0),
    audit_time         timestamp with time zone default current_timestamp not null,
    primary key(process_id, stage_name, pipeline_name)
) tablespace &table_tablespace;

-- @formatter:off

alter table pipelite2_stage add constraint ck_pipelite2_stage_result
check ( state in ('PENDING', 'ACTIVE', 'SUCCESS', 'ERROR') )
;

-- @formatter:on

create table pipelite2_stage_log
(
    pipeline_name varchar(256) not null,
    process_id    varchar(256) not null,
    stage_name    varchar(255) not null,
    stage_log     text,
    audit_time    timestamp with time zone default current_timestamp not null,
    primary key(process_id, stage_name, pipeline_name)
) tablespace &table_tablespace;

-- @formatter:off

-- @formatter:on

-- @formatter:off


CREATE FUNCTION pipelite2_stage_log_audit()
    RETURNS TRIGGER
    LANGUAGE PLPGSQL
AS $$
begin
    new.audit_time := current_timestamp;
end;
$$;

create trigger pipelite2_stage_log_audit
before insert or update or delete
on pipelite2_stage_log
for each row
execute procedure pipelite2_stage_log_audit();


-- @formatter:on


create table pipelite2_internal_error
(
    error_id      varchar(36) not null primary key,
    service_name  varchar(256) not null,
    pipeline_name varchar(256),
    process_id    varchar(256),
    stage_name    varchar(256),
    class_name    varchar(256) not null,
    error_time    timestamp with time zone not null,
    error_message text,
    error_log     text,
    audit_time    timestamp with time zone default current_timestamp not null
) tablespace &table_tablespace;

-- @formatter:off

-- @formatter:on
