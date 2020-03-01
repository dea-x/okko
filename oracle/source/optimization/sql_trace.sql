------------------------------------------------
-------------- sql_trace и tkprof --------------
------------------------------------------------

-- Включение отслеживания времени 
-- alter session set timed_statistics=true;
alter session set sql_trace=true;
------------------------------------------------
-- выполняем запросы, которые хотим проверить --
------------------------------------------------
alter session set sql_trace=false;


-- запрос для получения идентификатора серверного процесса (SPID — server process ID)
select a.spid
from v$process a, v$session b
where a.addr = b.paddr and b.audsid = userenv('sessionid')

-- полученный SPID будет в названии файла
-- Далее ищем в папке Oracle файл orcl_ora_{SPID}.trc
-- Возможно в начале будет дополнительный ноль, например orcl_ora_0{SPID}.trc
-- далее переходим в эту папки в консоли и запускаем tkprof
tkprof orcl_ora_{SPID}.trc result.txt