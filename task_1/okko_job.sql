BEGIN
  DBMS_SCHEDULER.DROP_JOB ('SYS.OKKO_JOB', force => true);
  DBMS_SCHEDULER.CREATE_JOB(
  JOB_NAME => 'okko_job',
  JOB_TYPE => 'PLSQL_BLOCK',
  JOB_ACTION => 'BEGIN test_user.okko.FILL_customers(1); test_user.okko.FILL_products(1); test_user.okko.FILL_fct_events; END;',
  START_DATE => SYSTIMESTAMP,
  REPEAT_INTERVAL => 'FREQ=MINUTELY; INTERVAL=5',
  END_DATE => SYSTIMESTAMP + INTERVAL '30' day,
  COMMENTS => 'Insert new data into source DB',
  ENABLED => TRUE);
END;
