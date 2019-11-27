#!/usr/bin/perl
use strict;
use warnings;
use 5.16.1;
use Config::ShellStyle;
use Data::Dumper;
use Zabbix::Sender;
use lib '/opt/Perl5/libs';
use ZAPI;

use constant {
  ZBX_API_CONFIG =>'/etc/zabbix/api/setenv.conf',
  ZHOST_ITEM_FMT => 'zhost.%s[%s]',
};

my @host_attrs = qw(maintenance_status);

my $zapi = ZAPI->new;
my $zbxEnv 	= $zapi->zenv;
my $dbh 	= $zapi->ldbh;

my $zsend = Zabbix::Sender->new(
  'server'	=> $zbxEnv->{'ZBX_SERVER'},
  'hostname'	=> $zbxEnv->{'ZBX_SERVER'}
);

$zsend->bulk_send(map {
 my $host = $_;
 my $hostName = $host->{'host'};
 map [sprintf(ZHOST_ITEM_FMT, $_, $hostName), $host->{$_}], @host_attrs
} @{$dbh->selectall_arrayref(sprintf(<<'EOSQL', join(',' => @host_attrs)), {Slice=>{}})});
SELECT host, %s FROM hosts WHERE status=0
EOSQL

say $zsend->_info;
