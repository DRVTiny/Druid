#!/usr/bin/perl
my $flDryRun=0;
use strict;
use open ':std', ':encoding(UTF-8)';
use utf8;
use v5.14.1;
use Carp qw(croak);
binmode *STDOUT,':utf8';
use constant {
     SETENV_FILE=>'/etc/zabbix/api/setenv_inframon.conf',
     SLA_ALGO_DO_NOT_CALC=>0,
     SLA_ALGO_ONE_FOR_PROBLEM=>1,
     SLA_ALGO_ALL_FOR_PROBLEM=>2,
     SHOW_SLA_DO_NOT_CALC=>0,
     SHOW_SLA_CALC=>1,
     HOSTS_TO_REMOVE_FROM_SVC=>0,
     HOSTS_TO_ADD_TO_SVC=>1,
     HOSTS_TO_SYNC_BTW_SVC_AND_HG=>2,
     TRIGS_TO_REMOVE_FROM_SVC=>0,
     TRIGS_TO_ADD_TO_SVC=>1,
     TRIGS_DONT_TOUCH=>2,
     TRIG_SEVERITY_INFO=>1,
     TRIG_SEVERITY_WARN=>2,
     TRIG_SEVERITY_ERROR=>3,
     SVC=>0,
     HOST=>1,
     BM_DONT_RENAME_ME=>128,
};
my %SETENV;
BEGIN {
 open (my $fhSetEnv,'<',SETENV_FILE) || die 'Cant set environment: '.SETENV_FILE.' not found!';
 %SETENV=map { chomp; $_=~m/^\s*(?<KEY>[A-Za-z0-9_-]+)\s*=\s*(?:(?<QUO>['"])(?<VAL>[^\g{QUO}]+?)\g{QUO}|(?<VAL>[^'"[:space:]]+?))\s*$/?($+{'KEY'},$+{'VAL'}):('NOTHING','NOWHERE') } grep { $_ !~ m/^\s*(?:#.*)?$/ } <$fhSetEnv>;
 push @INC,split(/\;/,$SETENV{'PERL_LIBS'}) if $SETENV{'PERL_LIBS'};
 close($fhSetEnv);
}

use Monitoring::Zabipi qw(zbx zbx_last_err zbx_api_url zbx_get_dbhandle);
use Monitoring::Zabipi::ITServices qw(doCreateITService doDeleteITService doDeleteLeafITService doRenameITService genITServicesTree getITService getAllITServiceDeps getITServiceDepsByType getITServiceChildren);
no warnings;
use Data::Dumper;
use JSON::XS qw(encode_json);
use Log::Log4perl;

Log::Log4perl::init_and_watch('/etc/itsvc/log4perl.conf',60);
sub err_ {
 Log::Log4perl::get_logger('ITSvc::Sync::Groups')->error(sprintf($_[0],@_[1..$#_]));
}
sub error_ {
 err_(@_);
}
sub warn_ {
 Log::Log4perl::get_logger('ITSvc::Sync::Groups')->warn(sprintf($_[0],@_[1..$#_]));
}
sub info_ {
 Log::Log4perl::get_logger('ITSvc::Sync::Groups')->info(sprintf($_[0],@_[1..$#_]));
}
sub fatal_ {
 Log::Log4perl::get_logger('ITSvc::Sync::Groups')->logcroak(sprintf($_[0],@_[1..$#_]));
}

my $firstarg=shift;
my $apiPars={
 'wildcards'=>'true',
 'dbDSN'=>sprintf('dbi:mysql:database=%s;host=%s',@SETENV{'DB_NAME','DB_HOST'}),
 'dbLogin'=>$SETENV{'DB_LOGIN'} || $SETENV{'DB_USER'},
 'dbPassword'=>$SETENV{'DB_PASSWD'} || $SETENV{'DB_PASS'} || $SETENV{'DB_PASSWORD'},
};

if ($firstarg eq '-x') {
 @{$apiPars}{qw(debug pretty)}=(1,1);
} else {
 unshift @ARGV,$firstarg;
}
die 'You must specify ZBX_HOST or ZBX_URL in your config '.SETENV_FILE 
 unless my $zbxConnectTo=$SETENV{'ZBX_HOST'} || $SETENV{'ZBX_URL'};
die 'Cant initialize API, check connecton parameters (ZBX_HOST or ZBX_URL) in your config '.SETENV_FILE
 unless Monitoring::Zabipi->new($zbxConnectTo, $apiPars);
zbx('auth',@SETENV{'ZBX_LOGIN','ZBX_PASS'}) || 
 die 'I cant authorize you on ',$zbxConnectTo,". Check your credentials and run this script with the first key '-x' to know why this happens exactly\n";

Monitoring::Zabipi::ITServices->init();
# Your code goes here ->
# For example, you may uncomment this line to get "Zabbix server" on STDOUT:
my $rootSvcID=shift || 0;
$flDryRun=1 if @ARGV;

my %txtTrgErrStatus=(
 'status'=>'was disabled',
 'state'=>'is in unknown state',
);

my @groups=getITServiceDepsByType($rootSvcID,'g');
fatal_ 'Cant get hostgroups under %s. Reason: %s', $rootSvcID, $groups[0]{'error'} if @groups==1 and exists($groups[0]{'error'});

my @syncedHosts;
for my $svcHG (@groups) {
 info_ 'Processing Hostgroup ITService: %s', $svcHG->{'name'};
 my ($groupid,$svcHGName)=@{$svcHG}{'groupid','name'};
 my ($hg)=@{zbx('hostgroup.get', {'groupids'=>$groupid,'output'=>['name']})};
 unless ($hg) {
  warn_ 'No such Zabbix group #%s which is referred by ITService %s. We need to remove this illegal ITService and proceed to next one', @{$svcHG}{'groupid','name'};
  doDeleteITService($svcHG->{'serviceid'}) unless $flDryRun;
  next
 }
 my $hgNameWOLeadingTags=($hg->{'name'}=~m/(?:(?:\[[^\]]+\])*\s*)?(.*)$/)[0];
 unless ($hg->{'name'} eq $svcHGName or $hgNameWOLeadingTags eq $svcHGName) {
  warn_ 'ITService name <<%s>> is not equal to hostgroup name <<%s>>', $svcHG->{'name'}, $hg->{'name'};
 } 
 my @svcsAssocWithHosts=grep {defined($_->{'ztype'}) and $_->{'ztype'} eq 'host'} @{getITServiceChildren($svcHG->{'serviceid'})};
 my @svcsNotRealHosts=grep !exists($_->{'hostid'}), @svcsAssocWithHosts;
 if (@svcsNotRealHosts) {
  warn_ 'We found that <<%s>> service depends on services which corresponds to not-existing-yet hosts: %s. We have to remove that services', $svcHG->{'name'}, join(','=>map '<<'.$_->{'name'}.'>>', @svcsNotRealHosts);
  unless ($flDryRun) {
   doDeleteITService($_) for map $_->{'serviceid'}, @svcsNotRealHosts;
  }
 }
 my %svcHosts=map {$_->{'hostid'}=>$_} grep exists $_->{'hostid'}, @svcsAssocWithHosts;
 my %hgHosts=map {$_->{'name'}||=$_->{'host'}; $_->{'hostid'}=>$_} @{zbx('host.get',{'groupids'=>$groupid,'output'=>['host','name'],'filter'=>{'status'=>0}})};
 my $diffHosts=getHashDiff(\%svcHosts,\%hgHosts);
 if (ref($diffHosts->[HOSTS_TO_REMOVE_FROM_SVC]) eq 'ARRAY' and @{$diffHosts->[HOSTS_TO_REMOVE_FROM_SVC]}) {
  for my $svc2del (map {$svcHosts{$_}} @{$diffHosts->[HOSTS_TO_REMOVE_FROM_SVC]}) {
   info_ 'Pretend to delete ITService << %s [%d] >>', @{$svc2del}{qw(name serviceid)};
   doDeleteITService($svc2del->{'serviceid'}) unless $flDryRun;
  }
 }
 
 if (ref($diffHosts->[HOSTS_TO_ADD_TO_SVC]) eq 'ARRAY' and @{$diffHosts->[HOSTS_TO_ADD_TO_SVC]}) {
  my $createHostSvcs=$flDryRun?sub {  }:\&genITServicesTree;
  $createHostSvcs->(
   {'serviceid'=>$svcHG->{'serviceid'},
    'nodes'=>{
     map {
      my $hostid=$_->{'hostid'};
      info_ 'Pretend to add ITService for host <<%s (%s)>>', @{$_}{'name','host'};
      sprintf('%s (h%d)', $_->{'name'}, $hostid)=>{
       'algorithm'=>SLA_ALGO_ONE_FOR_PROBLEM,
       'nodes'=>{
                   map {
                           '(t'.$_->{'triggerid'}.')'=>{
                                    'algorithm'=>SLA_ALGO_ONE_FOR_PROBLEM,
                                    'showsla'=>SHOW_SLA_DO_NOT_CALC,
                                    'triggerid'=>$_->{'triggerid'},
                           }	
                   } @{zbx('trigger.get',{'hostids'=>$hostid, 'monitored'=>1, 'min_severity'=>TRIG_SEVERITY_INFO, 'output'=>['triggerid']})}
       }, # <- trigger nodes under host
      } # <- 1 host node
     } @hgHosts{@{$diffHosts->[HOSTS_TO_ADD_TO_SVC]}}
    }, # <- host nodes
  });
  push @syncedHosts, @{$diffHosts->[HOSTS_TO_ADD_TO_SVC]};
 } # <- If hostgroup has newly-added hosts and we need to create apropriate/"associated" itservices for them
 
 my $syncHosts=$diffHosts->[HOSTS_TO_SYNC_BTW_SVC_AND_HG];
 next unless ref($syncHosts) eq 'ARRAY' and @{$syncHosts};
 
 my $hst2trgs=getTrigsOnHosts($syncHosts);
 
 syncSvcAndHost(@{$_}, $hst2trgs) for (map {[$svcHosts{$_},$hgHosts{$_}]} @{$syncHosts});  # <- Iterate hosts
 push @syncedHosts, @{$syncHosts};
} # <- Iterate groups

my %hostAlreadySeen=map {$_=>1} @syncedHosts;
exit unless my %segregHosts=map {$_->{'zobjid'}=>$_} grep {! exists $hostAlreadySeen{$_->{'zobjid'}} } getITServiceDepsByType($rootSvcID,'h');
if (my @hosts2wipe=grep !exists($_->{'hostid'}), values %segregHosts) {
 warn_ 'This out-of-groups hosts was removed: ('.join(','=>map {'s'.$_->{'serviceid'}.'=>'.'h'.$_->{'zobjid'}} @hosts2wipe).'), so corresponding services will be removed too';
 unless ($flDryRun) {
  doDeleteITService($_->{'serviceid'}) for @hosts2wipe;
 }
 delete @segregHosts{map $_->{'zobjid'}, @hosts2wipe};
 exit unless %segregHosts;
}

my $hst2trgs=getTrigsOnHosts(keys %segregHosts);
for my $svcHost (values %segregHosts) {
 info_ 'Found not synced host out of any groups: '.$svcHost->{'name'};
 syncSvcAndHost($svcHost, $svcHost, $hst2trgs);
}

sub getHashDiff {
  my ($lh,$rh)=@_;
  my (%hAux, @compRslt);
  $hAux{$_}|=1 				for keys %{$lh};
  $hAux{$_}|=2 				for keys %{$rh};
  push @{$compRslt[$hAux{$_}-1]},$_ 	for keys %hAux;
  \@compRslt;
}

sub getListDiff {
  my ($ll,$rl)=@_;
  my (%hAux, @compRslt);
  $hAux{$_}|=1 				for @{$ll};
  $hAux{$_}|=2 				for @{$rl};
  push @{$compRslt[$hAux{$_}-1]},$_ 	for keys %hAux;
  \@compRslt;
}

sub compSets {
  my ($ls,$rs)=@_;
  my (%hAux, @compRslt);
  $hAux{$_}|=1 				for ref($ls) eq 'ARRAY'?@{$ls}:keys %{$ls};
  $hAux{$_}|=2 				for ref($rs) eq 'ARRAY'?@{$rs}:keys %{$rs};
  push @{$compRslt[$hAux{$_}-1]},$_ 	for keys %hAux;
  $compRslt[$_]=[] for grep {! defined $compRslt[$_]} 0..2;
  \@compRslt;
}

sub getTrigsOnHosts {
 my $hostids=ref($_[0]) eq 'ARRAY'?shift:\@_;
 return { map {
  $_->{'hostid'}=>[
   map $_->{'triggerid'}, 
#    grep {!($_->{'status'}+$_->{'state'}) and $_->{'priority'}} 
    grep $_->{'priority'}, 
     @{$_->{'triggers'}}
  ]
 } @{
     zbx('host.get',{
      'hostids'=>$hostids,
#      'selectTriggers'=>[qw(status state priority)]
      'selectTriggers'=>['priority'],
     })
 }}
}

sub syncSvcAndHost {
  my ($svc,$host,$trigsOnHost)=@_;
  unless (defined $trigsOnHost and ref($trigsOnHost) eq 'HASH' and %{$trigsOnHost}) {
   $trigsOnHost=getTrigsOnHosts($host->{'hostid'});
  }
  my ($svcid,$svcName,$hostid,$hostName)=(@{$svc}{'serviceid','name'}, @{$host}{'hostid','name'});
  unless ( lc($svcName) eq lc($hostName) and !($svc->{'showsla'} & BM_DONT_RENAME_ME)) {
   my $newSvcName=(exists($host->{'host'}) and index($host->{'host'}, $hostName)<0 and index($host->{'host'}, lc($hostName))>=0)?lc($hostName):$hostName;
   info_ 'Host name seems to be changed, we need to rename <<%s>> to <<%s>>', $svcName, $newSvcName;
   doRenameITService( $svc->{'serviceid'}, 
                      (index($host->{'host'}, $hostName)<0 and index($host->{'host'}, lc($hostName))>=0)?lc($hostName):$hostName
   ) unless $flDryRun;
  }
  my %trg2svc=map {$_->{'triggerid'}=>$_->{'serviceid'}} grep exists($_->{'triggerid'}), @{getITServiceChildren($svcid)};
  my $diffTrigs=compSets(\%trg2svc, $trigsOnHost->{$hostid});
  next unless (ref $diffTrigs->[TRIGS_TO_REMOVE_FROM_SVC] eq 'ARRAY' and @{$diffTrigs->[TRIGS_TO_REMOVE_FROM_SVC]}) or (ref $diffTrigs->[TRIGS_TO_ADD_TO_SVC] eq 'ARRAY' and @{$diffTrigs->[TRIGS_TO_ADD_TO_SVC]});
  info_ 'ITService <<%s [%d]>> and host <<%s>> will be synced. %d differences in triggers configuration found: %d to remove from itsvc-host and %d to add to it', $svcName, $svcid, $hostName.($host->{'host'}?' ('.$host->{'host'}.')':'').' #'.$host->{'hostid'}, @{$diffTrigs->[TRIGS_TO_REMOVE_FROM_SVC]}+@{$diffTrigs->[TRIGS_TO_ADD_TO_SVC]}, scalar(@{$diffTrigs->[TRIGS_TO_REMOVE_FROM_SVC]}), scalar(@{$diffTrigs->[TRIGS_TO_ADD_TO_SVC]});
  for ( map [$trg2svc{$_},$_], @{$diffTrigs->[TRIGS_TO_REMOVE_FROM_SVC]} ) {
   info_ 'Removing ITService %s which corresponds to non-existing or disabled trigger %s', @{$_};
   doDeleteLeafITService($_->[0]) unless $flDryRun;
  }
  for my $trgid (@{$diffTrigs->[TRIGS_TO_ADD_TO_SVC]}) {
   info_ 'Creating IT Service below <<%s [%s]>> for trigger #%s',$svcName,$svcid,$trgid;
   unless ($flDryRun) {
    my $res=doCreateITService({
     'name'=>'(t'.$trgid.')',
     'algorithm'=>SLA_ALGO_ONE_FOR_PROBLEM,
     'showsla'=>SHOW_SLA_DO_NOT_CALC,
     'triggerid'=>$trgid,
     'sortorder'=>0,
     'parentid'=>$svcid,
    });
    if (!$res or my $err=eval { $res->{'error'} }) {
     error_ 'Cant create trigger-related ITService'.($err?": $err":'');
    } else {
     info_ 'ITService created: <<(t%s) [%s]>>', $trgid, $res->{'serviceids'}[0];
    }
   }
  } # <- Iterate over host triggers 
}

sub svc_create {
 my ($dbh, $svc)=@_;
 $dbh->do('LOCK TABLES ids WRITE');
 my $svcid=$dbh->selectall_arrayref(q(SELECT nextid FROM ids WHERE table_name='services' AND field_name='serviceid'))->[0][0]+1;
 $dbh->do(q(UPDATE ids SET nextid=).$svcid.q( WHERE table_name='services' AND field_name='serviceid'));
 $dbh->do('UNLOCK TABLES ids WRITE');
 $dbh->do(sprintf <<'EOSQL', join(','=>keys $svc), join(','=>map {$_ eq 'name'?$dbh->quote($svc->{$_}):$_} keys $svc));
INSERT INTO services (%s) VALUES (%s)
EOSQL
 return $svcid
}

END {
 zbx('logout') if zbx_api_url;
}
