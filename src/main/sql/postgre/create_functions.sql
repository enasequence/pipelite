-- @formatter:off

/* Retries process execution.
 */
create or replace function pipelite2_retry(
    v_pipeline_name in pipelite2_process.pipeline_name%type,
    v_process_id    in pipelite2_process.process_id%type)
returns void
as $$
declare
     v_rowcount integer;
begin
    update pipelite2_process
    set state = 'ACTIVE',
        exec_start = current_timestamp,
        exec_end = NULL
    where pipeline_name = v_pipeline_name and
          process_id = v_process_id and
          state in ('FAILED');

    get diagnostics v_rowcount := row_count;
    if v_rowcount <> 1 then
        raise exception 'Could not retry pipeline % process %', v_pipeline_name, v_process_id;
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
$$ LANGUAGE plpgsql;

/* Resets the process execution.
*/
create or replace function pipelite2_reset(
    v_pipeline_name in pipelite2_process.pipeline_name%type,
    v_process_id    in pipelite2_process.process_id%type)
returns void
as $$
declare
     v_rowcount integer;
begin
    update pipelite2_process
    set state = 'PENDING',
        exec_start = NULL,
        exec_end = NULL
    where pipeline_name = v_pipeline_name and
          process_id = v_process_id;

    get diagnostics v_rowcount := row_count;
    if v_rowcount <> 1 then
        raise exception 'Could not reset pipeline % process %', v_pipeline_name, v_process_id;
    end if;

    delete from pipelite2_stage
    where pipeline_name = v_pipeline_name and
          process_id = v_process_id;
end;
$$ LANGUAGE plpgsql;

/* Updates process priority.
 */
create or replace function pipelite2_prioritize(
    v_pipeline_name in pipelite2_process.pipeline_name%type,
    v_process_id    in pipelite2_process.process_id%type,
    v_priority      in pipelite2_process.priority%type )
returns void
as $$
declare
     v_rowcount integer;
begin
    update pipelite2_process
    set priority = v_priority
    where pipeline_name = v_pipeline_name and
          process_id = v_process_id;

    get diagnostics v_rowcount := row_count;
    if v_rowcount <> 1 then
        raise exception 'Could not prioritize pipeline % process %', v_pipeline_name, v_process_id;
    end if;
end;
$$ LANGUAGE plpgsql;

/* Cancels the process execution.
*/
create or replace function pipelite2_cancel(
    v_pipeline_name in pipelite2_process.pipeline_name%type,
    v_process_id    in pipelite2_process.process_id%type )
returns void
as $$
declare
     v_rowcount integer;
begin
    update pipelite2_process
    set state = 'CANCELLED'
    where pipeline_name = v_pipeline_name and
          process_id = v_process_id;

    get diagnostics v_rowcount := row_count;
    if v_rowcount <> 1 then
        raise exception 'Could not cancel pipeline % process %', v_pipeline_name, v_process_id;
    end if;
end;
$$ LANGUAGE plpgsql;

/* Removes the process execution.
*/
create or replace function pipelite2_remove(
    v_pipeline_name in pipelite2_stage.pipeline_name%type,
    v_process_id in pipelite2_stage.process_id%type )
returns void
as $$
begin
    delete from pipelite2_stage
    where pipeline_name = v_pipeline_name and
          process_id = v_process_id;

    delete from pipelite2_process
    where pipeline_name = v_pipeline_name and
          process_id = v_process_id;
end;
$$ LANGUAGE plpgsql;

/* Initialises the process execution.
 */
create or replace function pipelite2_init(
    v_pipeline_name in pipelite2_stage.pipeline_name%type,
    v_process_id in pipelite2_stage.process_id%type,
    v_priority in pipelite2_process.priority%type default 5)
returns void
as $$
declare
    v_row record;
begin
    for v_row in (
        select 1
        from pipelite2_process
        where pipeline_name = v_pipeline_name and
              process_id = v_process_id
     )
     loop
         raise exception 'Pipeline % process % already exists', v_pipeline_name, v_process_id;
     end loop;

    insert into pipelite2_process(pipeline_name, process_id, priority)
    values (v_pipeline_name, v_process_id, v_priority);
end;
$$ LANGUAGE plpgsql;

drop function pipelite2_retry;
drop function pipelite2_reset;
drop function pipelite2_prioritize;
drop function pipelite2_cancel;
drop function pipelite2_remove;
drop function pipelite2_init;

