--Процедура для записи логов--
CREATE OR REPLACE PROCEDURE cleaner_write_log (
	level_log           LOG_TABLE.LEVEL_LOG%TYPE,
	program_name        LOG_TABLE.PROGRAM_NAME%TYPE, 
        procedure_name      LOG_TABLE.PROCEDURE_NAME%TYPE, 
	message             LOG_TABLE.MESSAGE%TYPE) 
IS
PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    INSERT INTO LOG_TABLE VALUES (systimestamp, level_log, program_name, procedure_name, message);
    COMMIT;
END;

--Процедура очистки таблицы FCT_PROD если число строк больше 100000000--
create or replace PROCEDURE cleaner
AS 
    row_count 			number:=0;
    avg_date 			FCT_PROD.EVENT_TIME%TYPE;
    f_date 			FCT_PROD.EVENT_TIME%TYPE;
    l_date 			FCT_PROD.EVENT_TIME%TYPE;
    message                     VARCHAR2(1000);
BEGIN
   SELECT COUNT(*) INTO row_count FROM FCT_PROD;
   IF row_count > 100000000 then 
       SELECT MIN(EVENT_TIME) INTO f_date FROM FCT_PROD;
       SELECT MAX(EVENT_TIME) INTO l_date FROM FCT_PROD;
       avg_date := f_date + (l_date - f_date)/2;
       DELETE FCT_PROD WHERE EVENT_TIME < avg_date; 
   END IF; 
   COMMIT;
   EXCEPTION WHEN OTHERS THEN 
	   message := TO_CHAR(sqlcode)||'-'||sqlerrm||'. '||dbms_utility.format_error_backtrace;
       cleaner_write_log('ERROR', 'CLEAN_PROGRAM', 'CLEAN_PROCEDURE', message);
END; 
/
--Job--
BEGIN
  -- Job defined entirely by the CREATE JOB procedure.
  DBMS_SCHEDULER.create_job (
    job_name        => 'clean_job',
    job_type        => 'STORED_PROCEDURE',
    job_action      => 'clean',
    start_date      => SYSTIMESTAMP,
    repeat_interval => 'FREQ=WEEKLY; BYDAY=MON,FRI; BYHOUR=12; BYMINUTE = 00',
    end_date        => SYSTIMESTAMP + INTERVAL '30' day,
    enabled         => TRUE,
    comments        => 'Job defined entirely by the CREATE JOB procedure.');
END;
/
