Требования
==========
В тестах использовалась Ubuntu 18.04.

Apache с включенным mod_proxy.

Perl и его модули (список модулей и дополнительного ПО будет описан в разделе "Установка")

Druid можно располагать как на одном хосте с Zabbix, так и на каком-то отдельном. Взаимодействие с заббиксом происходит через API.

Установка
=========

Устанавливаем apach2 и активируем mod_proxy

	apt install apache2
	a2enmod proxy

Устанавливаем cpanminus и модули perl

	apt install cpanminus
	cpanm AnyEvent EV IO::Socket::SSL JSON Log4perl Log4perl::KISS Log::Dispatch Log::Log4perl Mojolicious Mojo::Log::Clearable Mojo::Redis Mojo::Redis2 POSIX::RT::Semaphore Redis::BCStation Redis::Fast Spread Tag::DeCoder Test Test::Pod Utils ZAPI

Устанавливаем Crystal

	wget https://github.com/crystal-lang/crystal/releases/download/0.31.1/crystal_0.31.1-1_amd64.deb
	dpkg -i https://github.com/crystal-lang/crystal/releases/download/0.31.1/crystal_0.31.1-1_amd64.deb

Клонируем Druid и вносим необходимые изменения

	git clone https://github.com/DRVTiny/Druid.git
	# Вместо 1.0.1 указываем актуальную версию
	cp -R /path_to_clonned_app/druid/* /opt/druid/1.0.1
	ln -s /opt/druid/1.0.1 /opt/druid/current
	chown -R zabbix. /opt/druid/1.0.1
	cp /opt/druid/current/contrib/redhat/*.service /etc/systemd/system
	systemctl daemon-reload
	# И запускаем все сервисы:
	service zapi start
	service druid-ng start
	service druid-calc-engine start

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
	tar -C /var/www/vhosts/${VHOST}/site xjfv /path_to_clonned_druid/contrib/frontend/rsdash.tbz2 
	chown -R www-data. /var/www/vhosts/${VHOST}
	a2ensite 00-example.com.conf
	service apache2 restart

Инструменты и команды
=====================

**it_service.pl** Основной инструмент, позволяющий управлять сервисами.

algo          - 
assoc         - сопоставление созданного сервиса с хостом или группой хостов
create        - создание сервиса
deassoc       - 
get           - 
help          - вывод списка этих команд, пока что не особо полезен
ln            - 
ls            - позволяет просматривать список созданных сервисов и выводит 
mv            - 
rename        - переименование сервиса
rm            - удаление сервиса
show          - 
unlink        - 

**sync_group_and_service.pl** Запускается после it_service.pl для синхронизации с заббиксом.

Может принимать в качестве аргумента serviceid, который необходимо синхронизировать, либо запускаться без аргументов для полной синхронизации.

**zobj_get.pl, sync-svc2zobj-tables.pl**

- ?

Примеры 
=======

Для создания нового сервиса выполним ряд команд:

	it_service.pl create "Test_service"
	it_service.pl assoc 9 g15   (где "9" - id сервиса Test_service, а g15 - id группы в zabbix)
	it_service.pl assoc 9 h1234 (h1234 - id хоста в zabbix)

И можно попробовать добавить дочерний сервис:

	it_service.pl create "Child_UMA" -p 1
	sync_group_and_service.pl (синхронизируем с заббиксом)
	service druid_calc_engine restart (необходимо рестартануть калькуляцию для получения обновлений из базы и можно наблюдать сервис в дашборде)

Удалить сервис можно так:

	it_service.pl rm 9
