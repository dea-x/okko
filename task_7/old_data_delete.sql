
DROP TABLE OLD_DATA_PROD PURGE;
--Таблица по удалёным данным--
create table OLD_DATA_PROD(f_date DATE, l_date DATE, proceeds NUMBER);

--Процедура вставки в таблицу OLD_DATA_PROD --
CREATE OR REPLACE PROCEDURE ODP_INSERT (  
f_dat OLD_DATA_PROD.f_date%TYPE,  
l_dat OLD_DATA_PROD.l_date%TYPE,  
proc OLD_DATA_PROD.proceeds%TYPE)    
IS  
BEGIN  
    INSERT INTO OLD_DATA_PROD VALUES (f_dat, l_dat, proc);  
    COMMIT;  
END;  
/  
--Процедура очистки таблицы FCT_PROD если число строк больше 1000000--
create or replace PROCEDURE cleaner
AS 
    row_count number:=0;
    procceds number:=0;
    avg_date FCT_PROD.EVENT_TIME%TYPE;
    f_date FCT_PROD.EVENT_TIME%TYPE;
    l_date FCT_PROD.EVENT_TIME%TYPE;
    VAR1 number;
    VAR2 number;
    VAR3 number;
    VAR4 number;
    VAR5 number;
    VAR6 number;
    VAR7 number;
BEGIN
   select count(*) into row_count from FCT_PROD;
   if row_count > 1000000 then 
       select EVENT_TIME into f_date from FCT_PROD where ROWNUM = 1;
       select max(EVENT_TIME) into l_date from FCT_PROD;
       avg_date := f_date + (l_date - f_date)/2;
       DELETE FCT_PROD where EVENT_TIME < avg_date; 
       select sum(SOLD) into procceds from SumPerDay where DAY BETWEEN to_char(f_date,'dd.mm.yyyy') and to_char(avg_date,'dd.mm.yyyy');
       ODP_INSERT(f_date, avg_date, procceds);
       SYS.dbms_space.unused_space('TEST_USER','FCT_PROD','TABLE',VAR1,VAR2,VAR3,VAR4,VAR5,VAR6,VAR7);
       EXECUTE IMMEDIATE 'ALTER TABLE FCT_PROD DEALLOCATE UNUSED KEEP'|| VAR3;
   end if; 
  COMMIT;
   EXCEPTION WHEN others THEN
   NULL;
END; 
--Для теста--
BEGIN
    cleaner;
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
