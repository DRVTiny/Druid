#!/usr/bin/perl -CDA
use strict;
use v5.14.1;
use utf8;
use constant {
     SETENV_FILE=>'/etc/zabbix/api/setenv_inframon.conf',
     TIMEZONE=>'MSK',
     DEFAULT_GOOD_SLA=>'99.05',
     SLA_ALGO_ALL_FOR_PROBLEM=>2,
};
my %SETENV;
BEGIN {
 open (my $fhSetEnv,'<',SETENV_FILE) || die 'Cant set environment: '.SETENV_FILE.' not found!';
 %SETENV=map { chomp; $_=~m/^\s*(?<KEY>[A-Za-z0-9_-]+)\s*=\s*(?:(?<QUO>['"])(?<VAL>[^\g{QUO}]+?)\g{QUO}|(?<VAL>[^'"[:space:]]+?))\s*$/?($+{'KEY'},$+{'VAL'}):('NOTHING','NOWHERE') } grep { $_ !~ m/^\s*(?:#.*)?$/ } <$fhSetEnv>;
 push @INC,split(/\;/,$SETENV{'PERL_LIBS'}) if $SETENV{'PERL_LIBS'};
 close($fhSetEnv);
}

use Monitoring::Zabipi qw(zbx zbx_last_err zbx_api_url zbx_get_dbhandle);
use Monitoring::Zabipi::ITServices;
use Getopt::Std qw(getopts);
use Data::Dumper;
use JSON::XS;
use Carp qw(croak);
no warnings;

my $firstarg=shift;
my $apiPars={
 'wildcards'=>'true',
 'dbDSN'=>sprintf('dbi:mysql:database=%s;host=%s',@SETENV{'DB_NAME','DB_HOST'}),
 'dbLogin'=>$SETENV{'DB_LOGIN'} || $SETENV{'DB_USER'},
 'dbPassword'=>$SETENV{'DB_PASSWD'} || $SETENV{'DB_PASS'} || $SETENV{'DB_PASSWORD'}, 
};
if ($firstarg eq '-x') {
 $apiPars->{'debug'}=1;
 $apiPars->{'pretty'}=1;
} else {
 unshift @ARGV,$firstarg;
}
die 'You must specify ZBX_HOST or ZBX_URL in your config '.SETENV_FILE 
 unless my $zbxConnectTo=$SETENV{'ZBX_HOST'} || $SETENV{'ZBX_URL'};
die 'Cant initialize API, check connecton parameters (ZBX_HOST or ZBX_URL) in your config '.SETENV_FILE
 unless Monitoring::Zabipi->new($zbxConnectTo, $apiPars);
zbx('auth',@SETENV{'ZBX_LOGIN','ZBX_PASS'}) || 
 die 'I cant authorize you on ',$zbxConnectTo,". Check your credentials and run this script with the first key '-x' to know why this happens exactly\n";
 
Monitoring::Zabipi::ITServices->init(zbx_get_dbhandle);
# Your code goes here ->
# For example, you may uncomment this line to get "Zabbix server" on STDOUT:
my %doSmth;
%doSmth=(
 'create'=>{
   'func'=>sub {
     my ($svcName,%opts)=@_;
     die 'You must specify service name' unless $svcName;
     my ($parentid)=defined($opts{'-p'})?($opts{'-p'}=~m/[^\d]/?(map $_->{'serviceid'}, @{zbx('service.get',{'search'=>{'name'=>$opts{'-p'}},'output'=>['serviceid']})}):($opts{'-p'})):undef;
     my $show_flags=($opts{'-f'}=~m/^\d{1,3}$/ and $opts{'-f'}<256)?$opts{'-f'}:0;
     my $algo=(exists($opts{'-a'}) and $opts{'-a'}!~m/[^\d]/ and $opts{'-a'}>=0 and $opts{'-a'}<=2)?$opts{'-a'}:SLA_ALGO_ALL_FOR_PROBLEM;
     doCreateITService({
      'name'=>$svcName,
      'algorithm'=>$algo,
      ($parentid?('parentid'=>$parentid):()),
      'goodsla'=>DEFAULT_GOOD_SLA,
      'showsla'=>$show_flags,
      'sortorder'=>0,
     });
   },
 },
 'get' => {
   'func'=>sub {
     my ($svc,%opts)=@_;
     my @getPars=(
      ( ( !exists($opts{'p'}) or $opts{'p'}=~m/[^\d]/ )?():('parentids'=>$opts{'p'}) ),
      ( $svc=~m/[^\d]/?('search',{'name'=>$svc}):('serviceids',$svc) ),
      'output'=>[qw(name triggerid showsla goodsla sortorder algorithm)],
      'selectParent'=>'extend',
     );
     
     return {'error'=>'No such ITService'}
      unless my ($zobj)=@{zbx('service.get',{@getPars})};
     
     if (ref($zobj->{'parent'}) eq 'ARRAY') {
      $zobj->{'parent'}={'serviceid'=>0,'name'=>'#'};
     } elsif (exists $zobj->{'parent'}{'triggerid'}) {
      delete $zobj->{'parent'}{'triggerid'};
     }

     if (exists($zobj->{'triggerid'}) and $zobj->{'triggerid'}) {
      ($zobj->{'trigger'})=@{zbx('trigger.get', {'triggerids'=>$zobj->{'triggerid'}, 'expandDescription'=>1, 'expandExpression'=>1, 'output'=>[qw(description expression value status state)], 'selectHosts'=>['name','host']})};
     } else {
      delete $zobj->{'triggerid'} if exists $zobj->{'triggerid'};
      if (!exists($opts{'C'}) and my @children=eval { map {utf8::decode($_->{'name'}); delete $_->{'triggerid'} unless $_->{'triggerid'}; $_ } @{getITServiceChildren($zobj->{'serviceid'})} } ) {
       $zobj->{'children'}=\@children;
      }      
     }
     doITServiceAddZOAttrs($zobj,1);
     return $zobj;
   },
   'opts'=>'pC',
 },
 'mv' => {
   'func'=>sub {
     die 'Wrong services passed to me' unless my @svcidsWhatWhere=getServiceIDsByNames(@_);   
     doMoveITService(@svcidsWhatWhere);
   },
 },
 'rm' => {
   'func'=>sub {
     die 'Wrong services passed to me' unless my @svcids=getServiceIDsByNames(@_);
     Monitoring::Zabipi::ITServices::doDeleteITService2( @svcids );
   }
 },
 'rename' => {
   'func'=>sub {
     my ($oldName,$newName)=@_;
     doRenameITService($oldName,$newName);
   },
 },
 'ln' => {
   'func'=>sub {
     die 'Wrong services passed to me' unless my @svcids=getServiceIDsByNames(@_);
     doSymLinkITService(@svcids);
   },
 },
 'ls' => {
   'func'=>sub {     
     for my $svc (scalar(@_)?@_:(0)) {
      my $svcid=$svc=~m/[^\d]/
       ? do { 
           say STDERR "Cant find service identified as $svc", next
            unless defined(my $n=getITServiceIDByName($svc)); 
           say "<< $svc [${n}] >>";
           $n
         }
       : do {
           say "<< [$svc] >>";
           $svc
         };
      my @svcChildren=eval { @{getITServiceChildren($svcid)} };
      my %trigid2name;
      if (my @trigids = map $_->{'triggerid'}, grep defined $_->{'triggerid'}, @svcChildren) {
       %trigid2name=map { $_->{'triggerid'}=>$_ } @{zbx('trigger.get', {'triggerids'=>\@trigids, 'expandDescription'=>0, 'output'=>['description','value']})}
      }
      
      say @svcChildren?join ("\n", sort { lc($a) cmp lc($b) } map {sprintf('%s [%d]', $_->{'triggerid'}?join('| ' => @{$trigid2name{$_->{'triggerid'}}}{'value','description'} ):$_->{'name'}, $_->{'serviceid'})} @svcChildren):'<empty>';
     }
   },
 },
 'unlink' => {
   'func'=>sub {
     doUnlinkITService(@_);
   },
 },
 'algo' => {
   'func'=>sub {
     setAlgoITService(@_);
   },
 },
 'assoc' => {
   'func'=>sub {    
     my $zobjid=pop @_;
     my @services2assoc=@_;
     croak 'Wrong services passed to me' unless my @svcids=getServiceIDsByNames(@services2assoc);
     my (@err,@assoc);
     for my $serviceid (@svcids) {
      my $rslt=doAssocITService($serviceid,$zobjid) ;
      if ( ref($rslt) eq 'HASH' and exists($rslt->{'error'}) ) {
       push @err, sprintf('Cant associate ITService %s with Zabbix object %s. Reason: %s',$serviceid,$zobjid,$rslt->{'error'});
      } else {
       push @assoc, $serviceid;
      }      
     }
     return {'associated'=>{'zobj'=>$zobjid, 'services'=>\@assoc}, @err?('errors'=>\@err):()}
   },
 },
 'deassoc' => {
   'func'=>sub {
     my $rslt=doDeassocITService(shift);
     croak 'Cant deassociate ITService: '.$rslt->{'error'} if exists($rslt->{'error'});
     return $rslt->{'result'}
   },
 },
 'show' => {
  'func'=>sub {
    my $subCmd=shift;
    for ($subCmd) {
     when (/associated/) {
      my $zobjid=shift;
      return getITServicesAssociatedWith($zobjid);
     }
     default {
      return {'error'=>'No such subcommand '.$subCmd};
     }
    }
  },
 },
 'help' => {
   'func'=>sub {
     my $topic=shift;
     unless ($topic) {
       my $cmdlist=join("\n\t"=>'',sort keys %doSmth),"\n";
       printf <<'EOUSAGE', $0, $cmdlist;
Usage:
 %s <COMMAND> [ARGUMENTS]
Where possible COMMANDs are:%s
EOUSAGE
     }
   },
 },
);

my $action=shift || 'help';
die 'No such action <<'.$action.'>>' unless my $hndl=$doSmth{$action};

my %act=('args'=>[],'pars'=>{});

if ($hndl->{'opts'} and @ARGV) {
 push @{$act{'args'}}, shift until (!@ARGV or substr($ARGV[0],0,1) eq '-');
 getopts($hndl->{'opts'},$act{'pars'});
} elsif (!$hndl->{'opts'}) {
 $act{'args'}=\@ARGV;
}

my $res=$hndl->{'func'}->(@{$act{'args'}}, %{$act{'pars'}});
print JSON::XS->new->pretty(1)->encode(ref($res)?$res:{'result'=>$res});

END {
 zbx('user.logout') if zbx_api_url();
}
