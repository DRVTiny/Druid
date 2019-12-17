#!/usr/bin/perl
use 5.16.1;
my $flDryRun = 0;
use strict;
use open ':std', ':encoding(UTF-8)';
use utf8;
use Carp qw(croak);
use constant {
    TIMEZONE                     => 'MSK',
    SETENV_FILE                  => '/etc/zabbix/api/setenv.conf',
    ZBX_AGENT_PORT               => 10050,
    ZBX_AGENT_WAIT_TIMEOUT       => 5,
    ROOT_SVC_NAME                => 'Абонентские комплекты',
    SLA_ALGO_DO_NOT_CALC         => 0,
    SLA_ALGO_ONE_FOR_PROBLEM     => 1,
    SLA_ALGO_ALL_FOR_PROBLEM     => 2,
    SHOW_SLA_DO_NOT_CALC         => 0,
    SHOW_SLA_CALC                => 1,
    DEFAULT_GOOD_SLA             => '99.05',
    IFACE_TYPE_ZABBIX_AGENT      => 1,
    IFACE_TYPE_SNMP              => 2,
    PROXY_NAME_SFX               => 'zbxprx02',
    COD_ID_TUSHINO               => 1,
    COD_ID_NUDOL                 => 2,
    HOSTS_TO_REMOVE_FROM_SVC     => 0,
    HOSTS_TO_ADD_TO_SVC          => 1,
    HOSTS_TO_SYNC_BTW_SVC_AND_HG => 2,
    TRIGS_TO_REMOVE_FROM_SVC     => 0,
    TRIGS_TO_ADD_TO_SVC          => 1,
    TRIGS_DONT_TOUCH             => 2,
    TRIG_SEVERITY_INFO           => 1,
    TRIG_SEVERITY_WARN           => 2,
    TRIG_SEVERITY_ERROR          => 3,
    SVC                          => 0,
    HOST                         => 1,
    FMT_TRIG_SVC_NAME		 => '(t%d)',
};
use Data::Dumper;

use FindBin;
use lib (
  $FindBin::RealBin . '/../lib/app',
  qw</opt/Perl5/libs /usr/local/share/perl5 /usr/local/lib64/perl5>,
  $FindBin::RealBin . '/../lib/cmn'
);

use Config::ShellStyle qw(read_config);
use Ref::Util qw(is_arrayref is_hashref);
use Monitoring::Zabipi qw(zbx zbx_last_err zbx_api_url zbx_get_dbhandle);
use Monitoring::Zabipi::ITServices 1.1;
no warnings;
use JSON::XS qw(encode_json);
use Log::Log4perl::KISS;
use Getopt::Std qw(getopts);

my %SETENV = %{ read_config(SETENV_FILE) };

getopts 'l:x' => \my %opt;
log_open( $opt{'l'} ) if $opt{'l'};

log_level($opt{'x'} ? 'DEBUG' : 'INFO');
my $apiPars = {
    'wildcards' => 'true',
    $opt{'x'} ? ( 'debug' => 1, 'pretty' => 1 ) : (),
};

logdie_( 'You must specify ZBX_HOST or ZBX_URL in your config %s', SETENV_FILE )
  unless my $zbxConnectTo = ( $SETENV{'ZBX_HOST'} // $SETENV{'ZBX_URL'} );
logdie_(
'Cant initialize API, check connecton parameters (ZBX_HOST or ZBX_URL) in your config %s. Error message: <<%s>>',
    SETENV_FILE, zbx_last_err() // 'NO_MESSAGE'
) unless Monitoring::Zabipi->new( $zbxConnectTo, $apiPars );
logdie_(
'I cant authorize you on %s. Check your credentials and rerun this script with the "-x" option to know why this happens exactly',
    $zbxConnectTo
) unless zbx( 'auth', @SETENV{qw/ZBX_LOGIN ZBX_PASS/} );

my $itsvc = Monitoring::Zabipi::ITServices->new;
my $zapi = $itsvc->zapi;

sub get_triggers_by_hostid {
  state $sthGetTrigger = $zapi->ldbh->prepare(sprintf <<'EOSQL', TRIG_SEVERITY_INFO);
SELECT 
  t.triggerid,
  t.priority
FROM 
  triggers t
    INNER JOIN functions f	USING(triggerid)
    INNER JOIN items i 		USING(itemid)
WHERE
  i.hostid=?
    AND
  t.priority > %d
EOSQL
  my ($hostid) = @_;
  $sthGetTrigger->execute($hostid);
  map $_->[0], @{$sthGetTrigger->fetchall_arrayref()};
}

# Your code goes here ->
# For example, you may uncomment this line to get "Zabbix server" on STDOUT:
my $rootSvcID = shift || 0;
$flDryRun = 1 if @ARGV;

my %txtTrgErrStatus = (
    'status' => 'was disabled',
    'state'  => 'is in unknown state',
);

my @groups = $itsvc->get_deps_by_type( $rootSvcID, 'g' );
logdie_ 'Cant get hostgroups under %s. Reason: %s', $rootSvcID,
  $groups[0]{'error'}
  if @groups == 1 and exists( $groups[0]{'error'} );

my @syncedHosts;
for my $svcHG (@groups) {

    # print Dumper $svcHG; exit;
    info_ 'Processing Hostgroup ITService: %s', $svcHG->{'name'};
    my ( $groupid, $svcHGName ) = @{$svcHG}{ 'groupid', 'name' };
    my ($hg) = @{
        zbx(
            'hostgroup.get', { 'groupids' => $groupid, 'output' => ['name'] }
        )
    };
    unless ($hg) {
        warn_
'No such Zabbix group #%s which is referred by ITService %s. We need to remove this illegal ITService and proceed to next one',
          @{$svcHG}{ 'groupid', 'name' };
        $itsvc->delete( $svcHG->{'serviceid'} ) unless $flDryRun;
        next;
    }
    my $hgNameWOLeadingTags =
      ( $hg->{'name'} =~ m/(?:(?:\[[^\]]+\])*\s*)?(.*)$/ )[0];
    unless ( $hg->{'name'} eq $svcHGName or $hgNameWOLeadingTags eq $svcHGName )
    {
        warn_ 'ITService name <<%s>> is not equal to hostgroup name <<%s>>',
          $svcHG->{'name'}, $hg->{'name'};
    }
    my @svcsAssocWithHosts =
      grep { defined( $_->{'ztype'} ) and $_->{'ztype'} eq 'host' }
      @{ $itsvc->get_children( $svcHG->{'serviceid'} ) };
    my @svcsNotRealHosts = grep !exists( $_->{'hostid'} ), @svcsAssocWithHosts;
    if (@svcsNotRealHosts) {
        warn_
'We found that <<%s>> service depends on services which corresponds to not-existing-yet hosts: %s. We have to remove that services',
          $svcHG->{'name'},
          join( ',' => map '<<' . $_->{'name'} . '>>', @svcsNotRealHosts );
        unless ($flDryRun) {
            $itsvc->delete($_) for map $_->{'serviceid'}, @svcsNotRealHosts;
        }
    }
    my %svcHosts = map { $_->{'hostid'} => $_ } grep exists $_->{'hostid'},
      @svcsAssocWithHosts;
    my %hgHosts =
      map { $_->{'name'} ||= $_->{'host'}; $_->{'hostid'} => $_ } @{
        zbx(
            'host.get',
            {
                'groupids' => $groupid,
                'output'   => [ 'host', 'name' ],
                'filter'   => { 'status' => 0 }
            }
        )
      };
    my $diffHosts = getHashDiff( \%svcHosts, \%hgHosts );
    if ( is_arrayref( $diffHosts->[HOSTS_TO_REMOVE_FROM_SVC] )
        and @{ $diffHosts->[HOSTS_TO_REMOVE_FROM_SVC] } ) {
        for my $svc2del ( map { $svcHosts{$_} }
            @{ $diffHosts->[HOSTS_TO_REMOVE_FROM_SVC] } ) {
            info_ 'Pretend to delete ITService << %s [%d] >>',
              @{$svc2del}{qw(name serviceid)};
            $itsvc->delete( $svc2del->{'serviceid'} ) unless $flDryRun;
        }
    }

    if ( is_arrayref( $diffHosts->[HOSTS_TO_ADD_TO_SVC] )
        and @{ $diffHosts->[HOSTS_TO_ADD_TO_SVC] } ) {

        my $svcs4hosts = +{
            'serviceid' => $svcHG->{'serviceid'},
            'nodes'     => {
                map {
                    my $hostid = $_->{'hostid'};
                    info_ 'Pretend to add ITService for host <<%s (%s)>>',
                      @{$_}{ 'name', 'host' };
                    sprintf( '%s (h%d)', $_->{'name'}, $hostid ) => {
                        'algorithm' => SLA_ALGO_ONE_FOR_PROBLEM,
                        'nodes'     => {
                            map {
                                  sprintf(FMT_TRIG_SVC_NAME, $_)
                                  => 
                                  +{
                                    'algorithm' => SLA_ALGO_ONE_FOR_PROBLEM,
                                    'showsla'   => SHOW_SLA_DO_NOT_CALC,
                                    'triggerid' => $_,
                                  }
                            } get_triggers_by_hostid($hostid)
                        },    # <- trigger nodes under host
                    }    # <- 1 host node
                } @hgHosts{ @{ $diffHosts->[HOSTS_TO_ADD_TO_SVC] } }
            },    # <- host nodes
        };
        $itsvc->gen_svc_tree_branch($svcs4hosts) unless $flDryRun;
        debug { 'SVCS4HOSTS =', Dumper($svcs4hosts) };
        push @syncedHosts, @{ $diffHosts->[HOSTS_TO_ADD_TO_SVC] };
    } # <- If hostgroup has newly-added hosts and we need to create apropriate/"associated" itservices for them

    my $syncHosts = $diffHosts->[HOSTS_TO_SYNC_BTW_SVC_AND_HG];
    next unless is_arrayref($syncHosts) and @{$syncHosts};

    my $hst2trgs = getTrigsOnHosts($syncHosts);

    syncSvcAndHost( @{$_}, $hst2trgs )
      for ( map { [ $svcHosts{$_}, $hgHosts{$_} ] } @{$syncHosts} )
      ;    # <- Iterate hosts
    push @syncedHosts, @{$syncHosts};
}    # <- Iterate groups

my %hostAlreadySeen = map { $_ => 1 } @syncedHosts;


my %segregHosts =
  map { $_->{'zobjid'} => $_ }
  grep { ! exists $hostAlreadySeen{ $_->{'zobjid'} } }
  $itsvc->get_deps_by_type( $rootSvcID, 'h' )
or exit;

if ( my @hosts2wipe = grep !exists( $_->{'hostid'} ), values %segregHosts ) {
    warn_ 'This out-of-groups hosts was removed: ('
      . join(
        ',' => map { 's' . $_->{'serviceid'} . '=>' . 'h' . $_->{'zobjid'} }
          @hosts2wipe )
      . '), so corresponding services will be removed too';
    unless ($flDryRun) {
        $itsvc->delete( $_->{'serviceid'} ) for @hosts2wipe;
    }
    delete @segregHosts{ map $_->{'zobjid'}, @hosts2wipe };
    exit unless %segregHosts;
}

my $hst2trgs = getTrigsOnHosts( keys %segregHosts );
for my $svcHost ( values %segregHosts ) {
    info_ 'Found not synced host out of any groups: ' . $svcHost->{'name'};
    syncSvcAndHost( $svcHost, $svcHost, $hst2trgs );
}

sub getHashDiff {
    my ( $lh, $rh ) = @_;
    my ( %hAux, @compRslt );
    $hAux{$_} |= 1 for keys %{$lh};
    $hAux{$_} |= 2 for keys %{$rh};
    push @{ $compRslt[ $hAux{$_} - 1 ] }, $_ for keys %hAux;
    \@compRslt;
}

sub getListDiff {
    my ( $ll, $rl ) = @_;
    my ( %hAux, @compRslt );
    $hAux{$_} |= 1 for @{$ll};
    $hAux{$_} |= 2 for @{$rl};
    push @{ $compRslt[ $hAux{$_} - 1 ] }, $_ for keys %hAux;
    \@compRslt;
}

sub compSets {
    my ( $ls, $rs ) = @_;
    my ( %hAux, @compRslt );
    $hAux{$_} |= 1 for is_arrayref($ls) ? @{$ls} : keys %{$ls};
    $hAux{$_} |= 2 for is_arrayref($rs) ? @{$rs} : keys %{$rs};
    push @{ $compRslt[ $hAux{$_} - 1 ] }, $_ for keys %hAux;
    $compRslt[$_] = [] for grep { !defined $compRslt[$_] } 0 .. 2;
    \@compRslt;
}

sub getTrigsOnHosts {
    my $hostids = ref( $_[0] ) eq 'ARRAY' ? shift : \@_;
    return {
        map {
            $_->{'hostid'} => [
                map $_->{'triggerid'},
                grep { $_->{'priority'} } @{ $_->{'triggers'} }
            ]
          } @{
            zbx(
                'host.get',
                {
                    'hostids'        => $hostids,
                    'selectTriggers' => [qw(priority)]
                }
            )
          }
    };
}

sub syncSvcAndHost {
    my ( $svc, $host, $trigsOnHost ) = @_;
    unless (defined $trigsOnHost
        and ref($trigsOnHost) eq 'HASH'
        and %{$trigsOnHost} ) {
        $trigsOnHost = getTrigsOnHosts( $host->{'hostid'} );
    }
    my ( $svcid, $svcName, $hostid, $hostName ) =
      ( @{$svc}{ 'serviceid', 'name' }, @{$host}{ 'hostid', 'name' } );
    unless ( lc($svcName) eq lc($hostName) and !( $svc->{'showsla'} & 128 ) ) {
        my $newSvcName =
          (       exists( $host->{'host'} )
              and index( $host->{'host'}, $hostName ) < 0
              and index( $host->{'host'}, lc($hostName) ) >= 0 )
          ? lc($hostName)
          : $hostName;
        info_
          'Host name seems to be changed, we need to rename <<%s>> to <<%s>>',
          $svcName, $newSvcName;
        $itsvc->rename(
            $svc->{'serviceid'},
            (
                      index( $host->{'host'}, $hostName ) < 0
                  and index( $host->{'host'}, lc($hostName) ) >= 0
            ) ? lc($hostName) : $hostName
        ) unless $flDryRun;
    }
    my %trg2svc = map { $_->{'triggerid'} => $_->{'serviceid'} }
      grep exists( $_->{'triggerid'} ), @{ $itsvc->get_children($svcid) };
    my $diffTrigs = compSets( \%trg2svc, $trigsOnHost->{$hostid} );
    next
      unless ( ref $diffTrigs->[TRIGS_TO_REMOVE_FROM_SVC] eq 'ARRAY'
        and @{ $diffTrigs->[TRIGS_TO_REMOVE_FROM_SVC] } )
      or ( ref $diffTrigs->[TRIGS_TO_ADD_TO_SVC] eq 'ARRAY'
        and @{ $diffTrigs->[TRIGS_TO_ADD_TO_SVC] } );
    info_
'ITService <<%s [%d]>> and host <<%s>> will be synced. %d differences in triggers configuration found: %d to remove from itsvc-host and %d to add to it',
      $svcName, $svcid,
      $hostName
      . ( $host->{'host'} ? ' (' . $host->{'host'} . ')' : '' ) . ' #'
      . $host->{'hostid'},
      @{ $diffTrigs->[TRIGS_TO_REMOVE_FROM_SVC] } +
      @{ $diffTrigs->[TRIGS_TO_ADD_TO_SVC] },
      scalar( @{ $diffTrigs->[TRIGS_TO_REMOVE_FROM_SVC] } ),
      scalar( @{ $diffTrigs->[TRIGS_TO_ADD_TO_SVC] } );
    for ( map [ $trg2svc{$_}, $_ ],
        @{ $diffTrigs->[TRIGS_TO_REMOVE_FROM_SVC] } ) {
        info_ 'Removing ITService %s which corresponds to non-existing or disabled trigger %s',
          @{$_};
        $itsvc->delete( $_->[0] ) unless $flDryRun;
    }
    for my $trgid ( @{ $diffTrigs->[TRIGS_TO_ADD_TO_SVC] } ) {
        info_ 'Creating IT Service below <<%s [%s]>> for trigger #%s',
          $svcName, $svcid, $trgid;
        unless ($flDryRun) {
            unless (
                my $svcTrig = $itsvc->create(
                    {
                        'name'      => '(t' . $trgid . ')',
                        'algorithm' => SLA_ALGO_ONE_FOR_PROBLEM,
                        'showsla'   => SHOW_SLA_DO_NOT_CALC,
                        'goodsla'   => DEFAULT_GOOD_SLA,
                        'triggerid' => $trgid,
                        'sortorder' => 0,
                        'parentid'  => $svcid,
                    }
                )
            ) {
                error_ 'Failed to create trigger-related ITService: triggerid=%d, parent_serviceid=%d', $trgid, $svcid;
            } else {
                info_ 'ITService created: <<(t%s) [%s]>>', $trgid,
                  $svcTrig->{'serviceids'}[0];
            }
        }
    }    # <- Iterate over host triggers
}

END {
    zbx('logout') if zbx_api_url;
}
