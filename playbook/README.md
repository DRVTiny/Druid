Ansible playbook for ubuntu.
===========================

Данный плейбук установит все необходимые пакеты и модули perl, подготовит директории и симлинки, склонирует все необходимое из репозитория и на выходе получится полностью готовый к работе Druid.


Использование
-------------

Достаточно добавить все параметры в файл group_vars/all, т.е. указать пользователя API Zabbix, БД, IP, хостнейм и т.д, а также в файле hosts указать пользователя, от имени которого будут выполняться команды, и имя хоста/IP, на котором будет производиться установка.

Раскатку можно запустить, например, таким образом:

	ansible-playbook -i hosts druid.yml -K
