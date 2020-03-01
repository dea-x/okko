# Описание

Для запуска задачи в _oozie_ необходимо как минимум 2 файла управления: _job.properties_ и _workflow.xml_.(Добавил еще _workflow.xml_-и c shell-запуском и параллельным запуском).
С этими файлами задача создается и выполняется сразу. Для использования таймера добавляется третий файл _coordinator.xml_ (Офрмляется отдельно для удобства, а так можно включить его в _workflow.xml_).
Теперь про каждый файл подробней:

#### job.properties

Файл, где нужно прописать все настройки (Необязательно выносить все сюда, но мне показалось, что так удобней)

##### Параметры:

_examplesRoot_ - часть пути которая часто повторяется, можно не использовать

_oozie.use.system.libpath_ - использовать ли библиотеки предустановленные на серве (true/false)

параметры workflow.xml:
 
_workflow_name_ - имя для workflow (String)

_nameNode_ - https://cwiki.apache.org/confluence/display/HADOOP2/NameNode 
(Наш: hdfs://cdh631.itfbgroup.local:8020)
  
_jobTracker_ - https://cwiki.apache.org/confluence/display/HADOOP2/JobTracker 
(Наш: cdh631.itfbgroup.local:8032)

_master_ - выбор запуска на кластере или локально (yarn или local[*]).При использовании local параметр mode надо удалить.

_mode_ - мод для кластера, не используется с local (cluster/client). Cluster-mod не работает, client-mod возможно равносилен локальному запуску.

_child_app_ - имя job-a? Работает в режиме кластера.

_queueName_ - имя очереди, по умолчанию дефолтное, лучше придумать свое.

_PythonPath,input-data,output-data,workflowPath_ - путь до python файла, input-output папки (spark), до workflow.xml

параметры coordinator.xml:

_startTime - endTime_ - нужынй период, работающий только в формате yyyy-MM-dd'T'HH:mm'Z'(нужно задавать время на 3 часа назад, чтобы обойти баг с timezone)

_step_ - шаг повторения, задается как int в минутах

_coordinator_name_ - имя для координатора

_timezone_ - временная зона (Наша: "Europe/Moscow" или "GMT+03:00"). Этот параметр не работает, возможно считывается временная зона сервера, которую нельзя изменить.

_oozie.coord.application.path_ - путь до coordinator.xml

# Запуск

один файл (job.properties) должен лежать на серве, остальные в hdfs:

Запускаем регулярное выполнение командой:

>oozie job -oozie http://hadoop.host.ru:11000/oozie -config job.properties -run

После запуска в консоль выведется job_id задачи. Используя этот job_id можно посмотреть информацию о статусе выполнения задачи:

>oozie job -info {job_id}

Остановить задачу:

>oozie job -kill {job_id}

Если Вы не знаете job_id задачи, то можно найти его, показав все регулярные задачи для вашего пользователя:

>oozie jobs -jobtype coordinator -filter user={user_name}