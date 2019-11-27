#!/usr/bin/perl
use 5.16.1;
use utf8;
use lib '/opt/Perl5/libs';
use constant {
    ZBX_API_CONF 	=> '/etc/zabbix/api/setenv_inframon.conf',
    BCST_NAME		=> 'zabbix',
    BCST_CHANNEL 	=> 'maint_status_changes',
    REDIS_SERVER 	=> 'redis://dc02-vm-inzbxfe01.dc.nspk.ru:6379',
};
use Data::Dumper;
use Scalar::Util qw(looks_like_number);
use ZAPI;
use DBR;
use Log4perl::KISS;
use Tag::DeCoder;
use Redis::BCStation;
mute_;

my $zapi = ZAPI->new;
my $dbr = DBR->new( my $dbh = $zapi->ldbh );

looks_like_number( my $eventID = $ARGV[0] )
    or logdie_ 'You must pass eventid as a first parameter';

my $eve = $zapi->selectall_arrayref(<<EOSQL, {'Slice' => {}}, $eventID)->[0] or logdie_ 'Cant find event or trigger object associated with the specified eventid=%d', $eventID;
SELECT 
    t.description 	"trgDescription",
    e.value		"eveType"
FROM
    events e
        INNER JOIN triggers t ON e.object=0 AND e.source=0 AND e.objectid=t.triggerid
WHERE
    eventid=?
EOSQL

my ($hostID) = ($eve->{'trgDescription'} =~ /id=(\d+)/)
    or logdie_ 'Trigger description is incorrect: it doesnt include hostid part';

my ($res) = $dbr->run_queries([<<EOSQL, {zabbix_server => $zapi->zenv->{'ZBX_SERVER'}, hostid => $hostID}]);
SELECT
    h.host,
    h.name,
    h.hostid,
    h.maintenance_status,
    {{__dbr_ternary(p.hostid IS NULL, '{{zabbix_server}}', p.host)}} monitored_from_host
FROM
    hosts h
        LEFT JOIN hosts p ON h.proxy_hostid IS NOT NULL AND h.proxy_hostid=p.hostid
WHERE
    h.hostid={{hostid}}
EOSQL

my $host = $res->[0]
    or logdie_ 'Zabbix host which hostid is mentioned in trigger description not found: maybe it was already deleted?';

my $bcst = Redis::BCStation->new(BCST_NAME, 'redis' => REDIS_SERVER);

$bcst->publish(BCST_CHANNEL() => encodeByTag('JS', $host), 
sub { 
    debug_ 'Published; ' . Dumper(\@_);
    Mojo::IOLoop->stop;
});
Mojo::IOLoop->start;
