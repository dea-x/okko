DROP TABLE OLD_DATA_PROD PURGE;
--Таблица по удалёным данным--
create table OLD_DATA_PROD(f_date DATE, l_date DATE, proceeds NUMBER);

--Процедура вставки в таблицу OLD_DATA_PROD --
CREATE OR REPLACE PROCEDURE ODP_INSERT (  
	f_dat 				OLD_DATA_PROD.f_date%TYPE,  
	l_dat 				OLD_DATA_PROD.l_date%TYPE,  
	proc 				OLD_DATA_PROD.proceeds%TYPE)    
IS  
BEGIN  
    INSERT INTO OLD_DATA_PROD VALUES (f_dat, l_dat, proc);  
    COMMIT;  
END;  
/  

--Процдура вставки в таблицу логов --
PROCEDURE cleaner_write_log (
	level_log           LOG_TABLE_TARGER.LEVEL_LOG%TYPE,
	program_name        LOG_TABLE_TARGER.PROGRAM_NAME%TYPE, 
	message             LOG_TABLE_TARGER.MESSAGE%TYPE) 
IS
BEGIN
    INSERT INTO LOG_TABLE_TARGET VALUES (systimestamp, level_log, program_name, message);
    COMMIT;
END;

--Процедура очистки таблицы FCT_PROD если число строк больше 1000000--
create or replace PROCEDURE cleaner
AS 
    row_count 			number:=0;
    procceds 			number:=0;
    avg_date 			FCT_PROD.EVENT_TIME%TYPE;
    f_date 				FCT_PROD.EVENT_TIME%TYPE;
    l_date 				FCT_PROD.EVENT_TIME%TYPE;
    VAR1 				number;
    VAR2 				number;
    VAR3 				number;
    VAR4 				number;
    VAR5 				number;
    VAR6 				number;
    VAR7 				number;
	message            VARCHAR2(1000);
BEGIN
   SELECT COUNT(*) INTO row_count FROM FCT_PROD;
   IF row_count > 1000000 then 
       SELECT MIN(EVENT_TIME) INTO f_date FROM FCT_PROD;
       SELECT MAX(EVENT_TIME) INTO l_date FROM FCT_PROD;
       avg_date := f_date + (l_date - f_date)/2;
       DELETE FCT_PROD WHERE EVENT_TIME < avg_date; 
       SELECT SUM(SOLD) INTO procceds FROM SumPerDay WHERE DAY BETWEEN to_char(f_date,'dd.mm.yyyy') AND to_char(avg_date,'dd.mm.yyyy');
       ODP_INSERT(f_date, avg_date, procceds);
       SYS.dbms_space.unused_space('TEST_USER','FCT_PROD','TABLE',VAR1,VAR2,VAR3,VAR4,VAR5,VAR6,VAR7);
       EXECUTE IMMEDIATE 'ALTER TABLE FCT_PROD DEALLOCATE UNUSED KEEP'|| VAR3;
   END IF; 
   COMMIT;
   EXCEPTION WHEN OTHERS THEN 
	   message := TO_CHAR(sqlcode)||'-'||sqlerrm||'. '||dbms_utility.format_error_backtrace;
       cleaner_write_log('ERROR', 'CLEAN_PROGRAM', message);
END; 

--Расписание для cleaner_job--
BEGIN
DBMS_SCHEDULER.CREATE_SCHEDULE
( schedule_name   => 'clean_shedule'
, start_date      => SYSTIMESTAMP
, repeat_interval => 'FREQ=WEEKLY; BYDAY=MON,THU; BYHOUR=18; BYMINUTE = 00'
);
END;
/
--Программа выполнения cleaner_job--
BEGIN
DBMS_SCHEDULER.CREATE_PROGRAM
( program_name  => 'clean_program'
, program_type  => 'STORED_PROCEDURE' 
, program_action => 'cleaner'
, enabled       => TRUE
);
END;
/
--cleaner_job--
BEGIN
DBMS_SCHEDULER.CREATE_JOB
( job_name      => 'cleaner_job'
, program_name  => 'clean_program'
, schedule_name => 'clean_shedule'
, enabled       => TRUE
);
END;
/
