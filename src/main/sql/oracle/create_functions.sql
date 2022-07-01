-- @formatter:off

/* Retries process execution.
 */
create or replace procedure pipelite2_retry(
    v_pipeline_name in pipelite2_process.pipeline_name%type,
    v_process_id    in pipelite2_process.process_id%type)
is
begin
    update pipelite2_process
    set state = 'ACTIVE',
        exec_start = current_timestamp,
        exec_end = NULL
    where pipeline_name = v_pipeline_name and
          process_id = v_process_id and
          state in ('FAILED');

    if sql%rowcount <> 1 then
        raise_application_error(-20102, 'Could not retry pipeline ' || v_pipeline_name || ' process ' || v_process_id);
    end if;

    update pipelite2_stage
    set ERROR_TYPE = null,
        STATE = 'PENDING',
        EXEC_END = null,
        EXEC_DATA = null,
        EXEC_NAME = null,
        EXEC_PARAMS = null,
        EXEC_RESULT_PARAMS = null,
        EXEC_START = null,
        EXEC_CNT = 0
    where pipeline_name = v_pipeline_name and
          process_id = v_process_id and
          state = 'ERROR';
end;
/

/* Resets the process execution.
*/
create or replace procedure pipelite2_reset(
    v_pipeline_name in pipelite2_process.pipeline_name%type,
    v_process_id    in pipelite2_process.process_id%type)
is
begin
    update pipelite2_process
    set state = 'PENDING',
        exec_start = NULL,
        exec_end = NULL
    where pipeline_name = v_pipeline_name and
          process_id = v_process_id;

    if sql%rowcount <> 1 then
        raise_application_error(-20102, 'Could not reset pipeline ' || v_pipeline_name || ' process ' || v_process_id);
    end if;

    delete from pipelite2_stage
    where pipeline_name = v_pipeline_name and
          process_id = v_process_id;
end;
/

/* Updates process priority.
 */
create or replace procedure pipelite2_prioritize(
    v_pipeline_name in pipelite2_process.pipeline_name%type,
    v_process_id    in pipelite2_process.process_id%type,
    v_priority      in pipelite2_process.priority%type )
is
begin
    update pipelite2_process
    set priority = v_priority
    where pipeline_name = v_pipeline_name and
          process_id = v_process_id;

    if sql%rowcount <> 1 then
        raise_application_error(-20102, 'Could not prioritize pipeline ' || v_pipeline_name || ' process ' || v_process_id);
    end if;
end;
/

/* Cancels the process execution.
*/
create or replace procedure pipelite2_cancel(
    v_pipeline_name in pipelite2_process.pipeline_name%type,
    v_process_id    in pipelite2_process.process_id%type )
is
begin
    update pipelite2_process
    set state = 'CANCELLED'
    where pipeline_name = v_pipeline_name and
          process_id = v_process_id;

    if sql%rowcount <> 1 then
        raise_application_error(-20102, 'Could not cancel pipeline ' || v_pipeline_name || ' process ' || v_process_id);
    end if;
end pipelite2_cancel;
/

/* Removes the process execution.
*/
create or replace procedure pipelite2_remove(
    v_pipeline_name in pipelite2_stage.pipeline_name%type,
    v_process_id in pipelite2_stage.process_id%type )
is
begin
    delete from pipelite2_stage
    where pipeline_name = v_pipeline_name and
          process_id = v_process_id;

    delete from pipelite2_process
    where pipeline_name = v_pipeline_name and
          process_id = v_process_id;
end;
/

/* Initialises the process execution.
 */
create or replace procedure pipelite2_init(
    v_pipeline_name in pipelite2_stage.pipeline_name%type,
    v_process_id in pipelite2_stage.process_id%type,
    v_priority in pipelite2_process.priority%type := 5)
is
begin
    for v_row in (
        select 1
        from pipelite2_process
        where pipeline_name = v_pipeline_name and
              process_id = v_process_id
     )
     loop
         raise_application_error(-20102, 'Pipeline ' || v_pipeline_name || ' process ' || v_process_id || ' already exists');
     end loop;

    insert into pipelite2_process(pipeline_name, process_id, priority)
    values (v_pipeline_name, v_process_id, v_priority);
end;
/

/* Initialises or resets the process execution.
 */
create or replace procedure pipelite2_init_or_reset(
    v_pipeline_name in pipelite2_stage.pipeline_name%type,
    v_process_id in pipelite2_stage.process_id%type,
    v_priority in pipelite2_process.priority%type default 5)
is
    v_init boolean := true;
begin
    for v_row in (
        select 1
        from pipelite2_process
        where pipeline_name = v_pipeline_name and
              process_id = v_process_id
     )
     loop
         v_init := false;
     end loop;

    if v_init
    then
        pipelite2_init(v_pipeline_name, v_process_id, v_priority);
    else
        pipelite2_reset(v_pipeline_name, v_process_id);
    end if;
end;
/



