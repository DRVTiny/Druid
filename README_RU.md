Требования
==========
В тестах использовалась Ubuntu 18.04.

Apache с включенным mod_proxy.

Perl и его модули (список модулей и дополнительного ПО будет описан в разделе "Установка")

Druid можно располагать как на одном хосте с Zabbix, так и на каком-то отдельном. Взаимодействие с заббиксом происходит через API.
В случае установки на отдельном хосте необходимо добавить пользователя, от имени которого будут запускаться сервисы Druid, в данной инструкции это будет zabbix.

Установка
=========

Устанавливаем apache2 и активируем mod_proxy

	apt install apache2
	a2enmod proxy_http

Создаем конфиг для zabbix API

	mkdir /etc/zabbix/api -p
	touch /etc/zabbix/api/setenv.conf

Содержимое конфига

	DB_NAME=zabbix
	DB_HOST=db_host
	DB_LOGIN=db-login
	DB_PASSWORD='db-password'
	ZBX_URL='http://zabbix-fqdn/zabbix/api_jsonrpc.php'
	ZBX_LOGIN='API_user'
	ZBX_PASS='password'
	ZBX_SERVER='x.x.x.x'

Клонируем Druid и вносим необходимые изменения

	mkdir /opt/druid/1.0.1 -p
	ln -s /opt/druid/1.0.1 /opt/druid/current
	# Вместо 1.0.1 указываем актуальную версию
	git clone https://github.com/DRVTiny/Druid.git /opt/druid/current
	chown -R zabbix. /opt/druid/1.0.1
	cp /opt/druid/current/contrib/redhat/*.service /etc/systemd/system
	systemctl daemon-reload

Устанавливаем необходимые пакеты

	apt install cpanminus libdbd-mysql-perl libmojolicious-perl libevent-dev libevent-extra-2.1-6 libevent-openssl-2.1-6 libevent-pthreads-2.1-6 libpcre16-3 libpcre3-dev libpcre32-3 libpcrecpp0v5 pkg-config libcrypto++-dev libcrypto++6 libssl-dev redis zlib1g-dev

Переносим perl-модуль из комлпекта поставки по одному из стандартных путей, например, таким образом

	mkdir /usr/local/lib/x86_64-linux-gnu/perl/5.26.1/Config/ -p
	cp /opt/druid/current/engine/lib/cmn/Config/ShellStyle.pm /usr/local/lib/x86_64-linux-gnu/perl/5.26.1/Config/

Устанавливаем модули perl

	cpanm Scalar::Util::LooksLikeNumber DBI JSON JSON::XS LWP::UserAgent URI::Encode boolean enum DBIx::Connector Log::Log4perl Mojo::Log::Clearable AnyEvent Mojo::Redis2 Redis::Fast Data::MessagePack
	
	cpanm --force POSIX::RT::Semaphore DBIx::SQLEngine

Создаем симлинк на hypnotoad

	ln -s /usr/bin/hypnotoad /usr/local/bin/hypnotoad

Создаем файл лога zapi и выдаем права пользователю

	mkdir /opt/druid/current/zapi/log && touch /opt/druid/current/zapi/log/app.log
	chown -R zabbix. /opt/druid/current/zapi/log

Устанавливаем Crystal и шарды, компилим druid-ng.

	wget https://github.com/crystal-lang/crystal/releases/download/0.31.1/crystal_0.31.1-1_amd64.deb
	dpkg -i crystal_0.31.1-1_amd64.deb
	cd /opt/druid/current && shards build
	
Создаем конфиг виртуального хоста в /etc/apache2/sites-available

	<VirtualHost example.com:80>
	  ServerName example.com
	  DocumentRoot /var/www/vhosts/example.com/site
	  ErrorLog /var/www/vhosts/example.com/log/error.log
	  CustomLog /var/www/vhosts/example.com/log/acces.log combined
	  ProxyPass /api http://localhost:3030 keepalive=on
	  ProxyPassReverse /api http://localhost:3030
	  ProxyPass /trg http://localhost:8099 keepalive=on
	  ProxyPassReverse /trg http://localhost:8099
	</VirtualHost>

Распаковываем фронтенд и применяем конфиг

	VHOST=example.com
	mkdir -p /var/www/vhosts/${VHOST}/{log,site}
	tar xjfv /opt/druid/current/contrib/frontend/rsdash.tbz2 -C /var/www/vhosts/${VHOST}/site 
	chown -R www-data. /var/www/vhosts/${VHOST}
	a2ensite 00-${VHOST}.conf
	service apache2 restart

Параметры фронтенда можно указать в config.js

	/var/www/vhosts/${VHOST}/site/js/rsdashboardpanes/config.js

Запускаем все сервисы:

	service zapi start
	service druid-ng start
	service druid-calc-engine start
	
Инструменты и команды
=====================

**it_service.pl** Основной инструмент, позволяющий управлять сервисами.

**itsvc** - симлинк на **it_service.pl**

Основной инструмент, позволяющий управлять сервисами.

*algo*

алгоритм вычисления состояния. 0 - не вычислять. 1 - WORST CASE, проблема, если хотя бы один потомок имеет проблему. 2 - AVERAGE CASE, степень дисфункции родителя равна среднему арифметическому от степени дисфункции потомков.

	itsvc algo $serviceid 2

*assoc*

сопоставление созданного сервиса с хостом или группой хостов

	itsvc assoc $serviceid $zabbix_groupid
	itsvc assoc $serviceid $zabbix_hostid

*create*

создание сервиса.

-p $serviceid - позволяет указать вышестоящий родительский сервис

-a $algo        - задает алгоритм и принимает значения 0,1 или 2

-f $flags        - число, биты в котором задают режимы работы с сервисом и его отображения. Пока поддерживаются только "0" (нет флагов) и "128" (не переименовывать при синхронизации - нужен для сервисов-хостов с именами на дашборде, отличающимися от имён соотв. привязанных к ним сервисов в Zabbix - чтобы процедура синхронизации не переименовывала такие сервисы автоматом) 

	itsvc create "Service_name"
	itsvc create "Child_service" -p  $serviceid
	itsvc create "Child_service" -p  $serviceid -a $algo -f $flags

*deassoc*

удаление сопоставления сервиса с хостом или группой. По сути осуществляет переименование IT-сервиса, удаляя «ассоциированную» часть 

	itsvc deassoc $serviceid

*get*

получение описания объекта-сервиса в JSON формате

	itsvc get $serviceid

*help*

справка по командам

*ln*

создать символическую ссылку на данный сервис под неким другим сервисом. Параметры: «serviceid того сервиса, на который создаётся ссылка», «serviceid сервиса, где ссылка появится». HINT: Создаётся именно символическая ссылка, т.е. в таблице services_links в колонке soft будет выставлена 1-ца. В контексте дашбордов симлинки отличаются от обычных связей "родитель"-"потомок" только тем, что сервис, находящийся на самом верхнем уровне, на который при этом сделан симлинк где-то в глубине дерева - будет корректно опознан как сервис верхнего уровня, потомок "<Root>" 

	itsvc ln $serviceid $serviceid_where_to_link

*ls*

позволяет просматривать список созданных сервисов, поддерживает несколько serviceid.
Не рекомендуется делать ls на сервисах-хостах, поскольку это приведёт к довольно медленному запросу в Zabbix API на получение описаний триггеров с "раскрытыми" значениями макросов. Т.е. ждать ответа, возможно, придётся очень долго.

	itsvc ls
	itsvc ls $serviceid
	itsvc ls $serviceid $serviceid ...

*mv*

перемещение сервиса «между» сервисами-родителями. Параметры: serviced перемещаемого сервиса, целевой serviceid

	itsvc mv $serviceid $parent_serviceid

*rename*

переименование сервиса. Переименование не приводит к смене ассоциированного объекта, хотя создать ассоциацию переименованием можно – но только в том случае, если ранее сервис был неассоциирован. 

	itsvc rename $serviceid 'Newname'

*rm*

удаление сервиса

	itsvc rm $serviceid
	itsvc rm $serviceid $serviceid ...

*show* 

на данный момент нет информации

*unlink*

убрать симлинк из-под определенного родителя или убрать симлинки из под всех родителей

	itsvc unlink $serviceid $parent_serviceid
	itsvc unlink $serviceid

**sync_group_and_service.pl** 

Запускается после it_service.pl для синхронизации с заббиксом.
Может принимать в качестве аргумента serviceid, который необходимо синхронизировать, либо запускаться без аргументов для полной синхронизации всех сервисов.

**zobj_get.pl**

вытаскивает объект из redis ровно в том виде, в котором он вытаскивается для показа на дашборде. Т.е. позволяет посмотреть "закешированное" состояние дерева сервисов.

**sync-svc2zobj-tables.pl**

поддерживает актуальность таблиц services_hosts, services_groups и прочих подобных, которые ускоряют работу с ассоциациями сервисов: не нужно искать по именам с регулярками, есть нормальные реляционные связи через id

Примеры 
=======

Для создания нового сервиса выполним ряд команд:

	itsvc create "Test_service"
	itsvc assoc 9 g15 -a 2   (где "9" - id, присвоеный сервису Test_service, а g15 - id группы в zabbix)
	itsvc assoc 9 h1234 (h1234 - id хоста в zabbix)

И можно попробовать добавить дочерний сервис:

	itsvc create "Child_Test" -p 9
	sync_group_and_service.pl (синхронизируем с заббиксом)
	service druid_calc_engine restart (необходимо рестартануть калькуляцию для получения обновлений из базы и можно наблюдать сервис в дашборде)

Удалить сервис можно так:

	itsvc rm 9
