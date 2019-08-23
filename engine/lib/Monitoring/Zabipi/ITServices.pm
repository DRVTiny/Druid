package Monitoring::Zabipi::ITServices;
use Monitoring::Zabipi qw(zbx zbx_get_dbhandle zbx_api_url zbx_last_err);

use v5.16.1;
use utf8;
binmode $_, ':utf8' for *STDOUT, *STDERR;
sub _E { {'error'=>@_>=2?sprintf($_[0],@_[1..$#_]):$_[0]} };
use constant {
     SLA_ALGO_DO_NOT_CALC=>0,
     SLA_ALGO_ONE_FOR_PROBLEM=>1,
     SLA_ALGO_ALL_FOR_PROBLEM=>2,
     SHOW_SLA_DO_NOT_CALC=>0,
     SHOW_SLA_CALC=>1,
     DEFAULT_GOOD_SLA=>'99.05',
     IFACE_TYPE_ZABBIX_AGENT=>1,
     IFACE_TYPE_SNMP=>2,
     HARD_LINK=>0,
     DONE => 1,
};
my %ltr2zobj=(
 'i'=>{ 'otype'=>'item',        'id_attr'=>'itemid',        'table'=>'items', 	 	'name'=>{'attr'=>'name'},  				},
 's'=>{ 'otype'=>'service',     'id_attr'=>'serviceid',     'table'=>'services', 	'name'=>{'attr'=>'name'},  				}, 
 't'=>{ 'otype'=>'trigger',     'id_attr'=>'triggerid',     'table'=>'triggers', 	'name'=>{'attr'=>'description'},			},
 'h'=>{ 'otype'=>'host',        'id_attr'=>'hostid',        'table'=>'hosts', 	 	'name'=>{'attr'=>[qw(host name)], 'fmt'=>'%s (%s)'}, 	},
 'g'=>{ 'otype'=>'hostgroup',   'id_attr'=>'groupid',       'table'=>'groups', 	 	'name'=>{'attr'=>'name'},				},
 'a'=>{ 'otype'=>'application', 'id_attr'=>'applicationid', 'table'=>'applications', 	'name'=>{'attr'=>'name'},				},
 'u'=>{ 'otype'=>'user',        'id_attr'=>'userid',        'table'=>'users',		'name'=>{'attr'=>[qw(alias name surname)], 'fmt'=>'%s (%s %s)'}, },
 'U'=>{ 'otype'=>'usergroup',   'id_attr'=>'usergroupid',   'table'=>'usrgrp',		'name'=>{'attr'=>'name'},    				},
 'm'=>{ 'otype'=>'mediatype',   'id_attr'=>'mediatypeid',   'table'=>'media_type',	'name'=>{'attr'=>'description'},			},
 'M'=>{ 'otype'=>'media',       'id_attr'=>'mediaid',       'table'=>'media',		'name'=>{'attr'=>'sendto'}, 				},
);
our $rxZOPfxs=join ''=>keys %ltr2zobj;
my $rxZOSfx=qr/\s*(\(([${rxZOPfxs}])(\d{1,10})\))$/;

use Exporter qw(import);
our @EXPORT_OK=qw(doDeleteITService doDeleteLeafITService genITServicesTree getITService getAllITServiceDeps doMoveITService getServiceIDsByNames doSymLinkITService chkZObjExists doAssocITService getITServicesAssociatedWith doCreateITService getITSvcChildrenByName);
our @EXPORT=qw($rxZOPfxs doDeleteITService doDeleteLeafITService doMoveITService doRenameITService getITService getITService4jsTree genITServicesTree getServiceIDsByNames doSymLinkITService doUnlinkITService getITSCache setAlgoITService chkZObjExists doAssocITService doDeassocITService getITServiceChildren getITServiceDepsByType doITServiceAddZOAttrs zobjFromSvcName getITServicesAssociatedWith getITServiceIDByName doCreateITService);

use DBI;
use Data::Dumper;
use Carp qw(confess);
use Scalar::Util qw(blessed);
use Try::Tiny;

#my @itsvcAttrs=qw(serviceid name algorithm triggerid showsla);
my %validSvcAttrs=(
  'serviceid'=>undef,
  'name'=>undef,
  'algorithm'=>undef,
  'sortorder'=>undef,
  'showsla'=>undef,
  'goodsla'=>99.9,
  'status'=>0,
  'triggerid'=>undef,
);
my @legalSvcAttrs=keys %validSvcAttrs;
my @mandSvcAttrs=qw(serviceid name algorithm sortorder showsla);

sub getSQLSvcAttrs {
 if (my $tbl=shift) {
  return join(','=>map $tbl.'.'.$_, @legalSvcAttrs)
 } else {
  return join(','=>@legalSvcAttrs)
 }
}

my %sql_=(
          'addSvc'=>{		'rq'=>['INSERT INTO services (%s) VALUES (%s)', join(','=>@legalSvcAttrs), join(','=>('?') x scalar(@legalSvcAttrs))],			},
          'addSvcLink'=>{ 	'rq'=>qq(INSERT INTO services_links (linkid,servicedownid,serviceupid,soft) VALUES (?,?,?,?)),						},
          'delLeafSvc'=>{	'rq'=>qq{DELETE s,slu,sld FROM services s LEFT JOIN services_links slu ON s.serviceid=slu.servicedownid LEFT JOIN services_links sld ON s.serviceid=sld.serviceupid WHERE s.serviceid=? AND (sld.linkid IS NULL OR sld.soft=1) },  },
          'getSvcDeps'=>{
                                'rq'=>[qq(select %s from services s inner join services_links l on s.serviceid=l.servicedownid where l.serviceupid=?), getSQLSvcAttrs('s')]
          },
          'getSvcHdDepsShort'=>{'rq'=>qq(select s.serviceid from services s inner join services_links l on s.serviceid=l.servicedownid where l.serviceupid=? and l.soft=0), },
          'getSvc'=>	{ 	'rq'=>[qq(select %s from services where serviceid=?),getSQLSvcAttrs()] 	},
          'chkSvcIDNameParent'=>{ 'rq'=>
            qq(
              select 
               count(s.serviceid)
              from 
               services s
                inner join services_links l on s.serviceid=l.servicedownid
              where
               s.serviceid=?
                and
               l.serviceupid=?
                and
               s.name regexp concat('^',?,'( \\\\([${rxZOPfxs}][0-9]+\\\\))?\$')
            ) },
          'chkSvcIDNameUnderRoot'=>{ 'rq'=>
            qq(
              select 
               count(s.serviceid)
              from 
               services s
                left join services_links l on s.serviceid=l.servicedownid and l.soft=0
              where
               s.serviceid=?
                and
               l.serviceupid is null
                and
               s.name regexp concat('^',?,'( \\\\([${rxZOPfxs}][0-9]+\\\\))?\$')
            ) },            
          'getSvcByZOExt' =>{	'rq'=>[qq(select %s from services where name like concat('% (',?,')')), getSQLSvcAttrs()] },
          'getSvcChildren'=>{	'rq'=>[qq(select %s from services_links l inner join services c on l.servicedownid=c.serviceid and l.serviceupid=?), getSQLSvcAttrs('c')] },
          'getSvcHardDeps'=>{	'rq'=>'select s.serviceid from services s inner join services_links l on l.servicedownid=s.serviceid and l.soft=0 where l.serviceupid=?' },
          'getRootSvcChildren'=>{ 
                                'rq'=>[qq(select %s from services s left outer join services_links l on l.servicedownid=s.serviceid and l.soft=0 where l.servicedownid is null), getSQLSvcAttrs('s')]
          },
          'isNotRootSvc?'=> { 	'rq'=>qq(select serviceupid from services_links where servicedownid=?) 			},
          'getTrg'=>	{ 	'rq'=>qq(select priority,value,status from triggers where triggerid=?),			},
          'mvSvc'=>	{ 	'rq'=>qq(update services_links set serviceupid=? where servicedownid=?), 		},
          'mvSvcFrom'=> { 	'rq'=>qq(update services_links set serviceupid=? where servicedownid=? and serviceupid=?), 	},
          'getSvcByName'=>{ 	'rq'=>qq(select serviceid from services where name=?),					},
          'getSvcByNameAndParentRX' => {
           'rq' => sprintf <<'EOSQL', $rxZOPfxs,
select
 s.serviceid
from
 services s
  inner join services_links sl on s.serviceid=sl.servicedownid
  inner join (select ? as name) ft on ft.name=s.name or (s.name like concat(ft.name, '%%') and s.name regexp '\\\([%s][0-9]+\\\)$')
where
 sl.serviceupid=?
EOSQL
          },
          'getSvcByNameUnderRootRX'=>{ 'rq'=>qq(select s.serviceid from services s left  join services_links sl on s.serviceid=sl.servicedownid where s.name regexp concat('^',?,'( \\\\([${rxZOPfxs}][0-9]+\\\\))?\$') and sl.serviceupid is null), },
          'getSvcByNameAndParent'=>{ 'rq'=>qq(select s.serviceid from services s inner join services_links sl on s.serviceid=sl.servicedownid where s.name=? and sl.serviceupid=?      ), },
          'getSvcByNameUnderRoot'=>{ 'rq'=>qq(select s.serviceid from services s left  join services_links sl on s.serviceid=sl.servicedownid where s.name=? and sl.serviceupid is null), },          
          'renSvcByName'=>{ 	'rq'=>qq(update services set name=? where name=?),					},
          'renSvcByID'	=>{ 	'rq'=>qq(update services set name=? where serviceid=?),					},
          'unlinkSvc'	=>{	'rq'=>qq(delete from services_links where serviceupid=? and servicedownid=?),		},
          'algochgSvc'	=>{	'rq'=>qq(update services set algorithm=? where serviceid=?),				},
          'checkHostEnabled'=>{	'rq'=>qq(select if(maintenance_status=1 or status=1,0,1) flHostMonStatus from hosts where hostid=?)	},

);

my %idOps=(
  'services'=>{
    'id_attr'=>'serviceid'
  },
  'services_links'=>{
    'id_attr'=>'linkid'
  },
);

my $dbhGlob;
sub init {
 my ($slf,$dbh)=@_; 
 do { $dbh=$slf; $slf=undef } if ref($slf) eq 'DBI::db';
 $dbh||=zbx_get_dbhandle;
 return {'error'=>'You must specify dbh handler as a first parameter'} 
  unless $dbh and ref($dbh) and blessed($dbh) and eval { $dbh->can('prepare') }; 
 $_->{'st'}=$dbh->prepare(
  ref($_->{'rq'}) eq 'ARRAY'
   ? ($_->{'rq'}=sprintf($_->{'rq'}[0],@{$_->{'rq'}}[1..$#{$_->{'rq'}}]))
   : (ref($_->{'rq'})?confess('Incorrectly filled %sql_ detected'):$_->{'rq'})
 ) for values %sql_;
 while (my ($tblName, $tblProps)=each %idOps) {
   my @bindParams=map $dbh->quote($_), $tblName, $tblProps->{'id_attr'};
   $tblProps->{'sthBlock'}=$dbh->prepare(sprintf <<'EOSQL', @bindParams);
SELECT nextid FROM ids WHERE table_name=%s AND field_name=%s FOR UPDATE
EOSQL
   $tblProps->{'getNextId'}=sub {
     return unless $tblProps->{'sthBlock'}->execute();
     return $tblProps->{'sthBlock'}->fetchall_arrayref()->[0][0]+1;
   };
   $tblProps->{'sthIncr'}=$dbh->prepare(sprintf <<'EOSQL', @bindParams);
UPDATE ids SET nextid=nextid+? WHERE table_name=%s AND field_name=%s
EOSQL
   $tblProps->{'incrNextId'}=sub {
     return unless $tblProps->{'sthIncr'}->execute(defined($_[0])?$_[0]:1);
     return $tblProps->{'sthIncr'}->rows || '0E0';
   };
 } 
 for my $zo (values %ltr2zobj) {
  my @zoNameAttrs=(ref($zo->{'name'}{'attr'}) eq 'ARRAY')?@{$zo->{'name'}{'attr'}}:($zo->{'name'}{'attr'});
  for my $what ('name','zobj') {
   my $query=$zo->{$what}{'query'}=sprintf(
    'SELECT %s FROM %s WHERE %s=?',
     join(','=>@zoNameAttrs,$what eq 'zobj'?($zo->{'id_attr'}):()),
     @{$zo}{'table','id_attr'},
   );
   $zo->{$what}{'st'}=$dbh->prepare($query);
  }
  $zo->{'name'}{'get'}=sub {
   $zo->{'name'}{'st'}->execute(shift);
   my @res=map {utf8::decode($_); $_} @{$zo->{'name'}{'st'}->fetchall_arrayref([])->[0]};
   $zo->{'name'}{'fmt'}?
    sprintf($zo->{'name'}{'fmt'}, @res)
                       :
    join(' '=>@res)    ;
  };
  $zo->{'zobj'}{'get'}=sub {
   $zo->{'zobj'}{'st'}->execute(shift);
   return unless my $zobj=$zo->{'zobj'}{'st'}->fetchall_arrayref({})->[0];
   for (@zoNameAttrs) {
    utf8::decode($zobj->{$_}) unless utf8::is_utf8($zobj->{$_});
   }
   return $zobj;
  };
  $zo->{'name'}{'update'}=sub {
   my ($objid,$newName)=@_;
   my $stRename=$dbh->prepare(sprintf('UPDATE %s SET %s=? WHERE %s=?', $zo->{'table'}, (ref($zo->{'name'}{'attr'})?$zo->{'name'}{'attr'}[0]:$zo->{'name'}{'attr'}), $zo->{'id_attr'}));
   $stRename->execute($newName, $objid);
  };
  my $st=$dbh->prepare(sprintf('SELECT 1 FROM %s WHERE %s=?', @{$zo}{'table','id_attr'}));
  $zo->{'check'}{'exists'}=sub {
   return undef unless $_[0]=~/^\d{1,10}$/;
   $st->execute(shift);
   $st->fetchrow_array()
  };
 } 
 $dbhGlob=$dbh;
}

sub doLinkITService {
 my ($what, $where, $soft)=@_;
 my $svAutoCommit=$dbhGlob->{'AutoCommit'};
 $dbhGlob->{'AutoCommit'}=0 unless defined $svAutoCommit and $svAutoCommit==0; 
 $dbhGlob->do('START TRANSACTION');
 my $curLinkId=$idOps{'services_links'}{'getNextId'}->();
 $idOps{'services_links'}{'incrNextId'}->(1);
 $sql_{'addSvcLink'}{'st'}->execute($curLinkId,$what,$where,$soft?1:0);
 $dbhGlob->commit;
 $dbhGlob->{'AutoCommit'}=$svAutoCommit unless !defined $svAutoCommit or $svAutoCommit==$dbhGlob->{'AutoCommit'};
}

sub doCreateITService {
  my ($svcs)=@_;
  return {'error'=>'You must specify services to create'}
    unless ref($svcs) and ((ref($svcs) eq 'ARRAY' and @$svcs) or (ref($svcs) eq 'HASH' and %$svcs));
  $svcs=[$svcs] if ref($svcs) eq 'HASH';
  my $res;
  my ($sthAddService,$sthAddSvcLink)=map $_->{'st'}, @sql_{'addSvc','addSvcLink'};
  my $saveAutoCommit=$dbhGlob->{'AutoCommit'};
  $dbhGlob->{'AutoCommit'}=0;
  $dbhGlob->do('START TRANSACTION');
  try {
    die 'Cant get nextid for table "services"'.($dbhGlob->errstr?': '.$dbhGlob->errstr:'')
      unless my $curSvcId=$idOps{'services'}{'getNextId'}->();
    # All... or nothing
    $idOps{'services'}{'incrNextId'}->(scalar @{$svcs});
    my $curLinkId;
    if (my $countLinks=grep defined($_->{'parentid'}), @{$svcs}) {
      die 'Cant get nextid for table "services_links"'.($dbhGlob->errstr?': '.$dbhGlob->errstr:'')
        unless $curLinkId=$idOps{'services_links'}{'getNextId'}->();
      $idOps{'services_links'}{'incrNextId'}->($countLinks)
    }
    for my $svc (@{$svcs}) {
      my $parSvcId=$svc->{'parentid'};
      die 'Service doesnot contain any valid attributes'
        unless %{$svc}=map {$_=>$svc->{$_}} grep exists($validSvcAttrs{$_}), keys $svc;
      my $serviceid=$svc->{'serviceid'}=$curSvcId++;
      
      if (my @attrsMiss=grep !defined($svc->{$_}), @mandSvcAttrs) {
        die 'Failed to create service <<'.($svc->{'name'} || 'UNKNOWN').'>> : mandatory attribute'.($#attrsMiss?'s':'').' missed: '.join(','=>@attrsMiss)
      }
      
      die 'Failed to create service <<'.$svc->{'name'}.'>>: database error'.($dbhGlob->errstr?': '.$dbhGlob->errstr:'')
        unless $sthAddService->execute(map {defined($svc->{$_})?$svc->{$_}:$validSvcAttrs{$_}} @legalSvcAttrs);
      
      if ($parSvcId) {
        $sthAddSvcLink->execute($curLinkId++,$serviceid,$parSvcId,HARD_LINK)
          or die 'Cant link <<'.$svc->{'name'}.'>> to parent service #'.$parSvcId.': database error'.($dbhGlob->errstr?': '.$dbhGlob->errstr:'');
      }
    }
    $dbhGlob->commit or die 'Commit service changes failed: '.$dbhGlob->errstr;
    $res={'serviceids'=>[map $_->{'serviceid'}, @{$svcs}]}
  } catch {
    $res={'error'=>$_};
    $dbhGlob->rollback;
  } finally {
    $dbhGlob->{'AutoCommit'}=$saveAutoCommit 
      unless $dbhGlob->{'AutoCommit'}==$saveAutoCommit;
  };
  return $res
}

sub chkITServiceExists {
 $ltr2zobj{'s'}{'check'}{'exists'}->(shift);
}

sub getITServiceChildren {
 my ($svcid, $flResolveZOName) = @_;
# Return if svcid defined, but contains anything "non-numeric" 
 return if $svcid and (ref($svcid) or $svcid=~/[^\d]/);
 my $st=$svcid
  ? do {
     $sql_{'getSvcChildren'}{'st'}->execute($svcid);
     $sql_{'getSvcChildren'}{'st'}
    }
  : do {
# Special case: svcid can be "0". It is fake serviceid appropriate to non-existing "<Common Root>" service
     $sql_{'getRootSvcChildren'}{'st'}->execute();
     $sql_{'getRootSvcChildren'}{'st'}
    }; 
 my @svcChildren=map { my $chldSvc=$_; utf8::is_utf8($chldSvc->{'name'}) or utf8::decode($chldSvc->{'name'}); doITServiceAddZOAttrs($chldSvc,$flResolveZOName) } @{$st->fetchall_arrayref({})};
 
 wantarray 
  ? return(@svcChildren)
  : return(\@svcChildren);
}

sub getITServiceHardDeps {
 confess 'You must specify serviceid>0' unless $_[0];
 my $st=$sql_{'getSvcHdDepsShort'}{'st'};
 $st->execute($_[0]);
 return 
   $st->rows
        ? [map $_->[0], @{$st->fetchall_arrayref([])}]
        : []
}

sub doDeleteITService {
 my ($serviceid,%opts)=@_;
 my $stGetHardDeps=$sql_{'getSvcHdDepsShort'}{'st'};
 sub getSvcDeps {
  my $svcid=shift;
  $stGetHardDeps->execute($svcid);
  (map getSvcDeps($_->[0]), @{$stGetHardDeps->fetchall_arrayref()}),$svcid;
 }
 my @deps=getSvcDeps($serviceid);
 zbx('service.deletedependencies',\@deps);
 zbx('service.delete',\@deps);
}

sub doDeleteLeafITService {
 confess 'You must specify serviceid!' unless my $svcid=shift;
 try {
  $sql_{'delLeafSvc'}{'st'}->execute($svcid) or die $dbhGlob->errstr;
  die 'No such LEAF it service #'.$svcid
   unless my $rows=$sql_{'delLeafSvc'}{'st'}->rows;
  $dbhGlob->commit unless $dbhGlob->{'AutoCommit'};
  return $rows?{'serviceids'=>[$svcid]}:{'error'=>'No such LEAF it service #'.$svcid}
 } catch {
  return {'error'=>$_}
 };
}

sub doDeleteITService2 {
 die 'You must pass serviceid of the it service to be removed. This serviceid cant be zero (root)!' 
  unless my $svcid=shift;
  my (@delSvcs,$r);
# Recursive delete all hard-dependencies (children)
 for my $childSvcId ( @{getITServiceHardDeps($svcid)} ) {
  $r=doDeleteITService2($childSvcId);
  die 'Failed to remove IT service #'.$childSvcId.': '.$r->{'error'} 
   if $r->{'error'};
  push @delSvcs, @{$r->{'serviceids'}};
 }
# Than, delete IT service itself
 $r=doDeleteLeafITService($svcid);
 confess 'Failed to remove IT service #'.$svcid.(ref $r eq 'HASH'?': '.$r->{'error'}:'')
  if ref($r) ne 'HASH' or $r->{'error'};
 return {'serviceids'=>[@delSvcs,$svcid]};
}

sub doMoveITService {
 my $what2mv = $_[0];
 my $where2place = $_[$#_];
 (my $st = $sql_{'isNotRootSvc?'}{'st'})->execute($what2mv);
 $st->rows
 ? do {
     # Nothing to do if moved service is already under target service
    +{map { $_->[0] => 1 } @{$st->fetchall_arrayref()}}->{$where2place} and return DONE;
    if ( @_ == 3 ) {
     $sql_{'mvSvcFrom'}{'st'}->execute($where2place, $what2mv, $_[1]);
    } elsif ( @_ == 2 ) {
     $sql_{'mvSvc'}{'st'}->execute($where2place, $what2mv);
    }
   } 
 : zbx('service.adddependencies',{
    'serviceid' => $where2place,
    'dependsOnServiceid' => $what2mv,
    'soft' => HARD_LINK,
   })
}

sub zobjFromSvcName { 
 if (ref $_[0] eq 'SCALAR') {
  ${$_[0]}=~s%${rxZOSfx}%%;
  return wantarray?($2,$3):$1
 } else {
  return ($_[0]=~$rxZOSfx)[wantarray?(1,2):(0)]  
 }
}

sub doITServiceAddZOAttrs {
 my ($svc,$flResolveZOName)=@_;
 return undef unless ref($svc) eq 'HASH' and exists($svc->{'name'}) and exists($svc->{'serviceid'});
 my ($zoltr, $oid)=$svc->{'triggerid'}
  ?  ('t',$svc->{'triggerid'})
  : do {
     return $svc unless $svc->{'name'}=~s%${rxZOSfx}%%;
     ($2,$3)
    };
 my $hndlZO=$ltr2zobj{$zoltr};
 @{$svc}{'ztype','zobjid'}=($hndlZO->{'otype'},$oid);
 return $svc unless my $zobj=$hndlZO->{'zobj'}{'get'}->($oid);
 $svc->{$hndlZO->{'id_attr'}}=$oid;
 $svc->{'zobj'}=$zobj if $flResolveZOName;
 return $svc;
}

sub doRenameITService {
 my ($from,$to)=@_;
 my $flFromIsName=$from=~m/[^\d]/;
 unless ($flFromIsName) {
  return {'error'=>'No such IT Service'} unless my $svcName=$ltr2zobj{'s'}{'name'}{'get'}->($from);
  if (my $zoSfx=zobjFromSvcName($svcName) and !zobjFromSvcName($to)) {
   $to.=' '.$zoSfx;
  }
 }
 $sql_{'renSvcBy'.($flFromIsName?'Name':'ID')}{'st'}->execute($to,$from);
}

sub doSymLinkITService {
 my @svcids=@_;
 return undef unless @_>1;
 my $where2link=pop @svcids;
 for my $what2link (@svcids) {
  zbx('service.adddependencies',{'soft'=>1,'serviceid'=>$where2link,'dependsOnServiceid'=>$what2link});
 }
}

sub doUnlinkITService {
 my ($where,$what)=@_;
 $sql_{'unlinkSvc'}{'st'}->execute($where,$what);
}

sub setAlgoITService {
 my ($serviceid,$newalgo)=@_;
 $sql_{'algochgSvc'}{'st'}->execute($newalgo,$serviceid);
}

sub doAssocITService {
 my ($svcid,$zobjid)=@_;
 return {'error'=>'No such Zabbix object: '.$zobjid} 	unless chkZObjExists($zobjid);
 return {'error'=>'No such IT Service: '.$svcid}   	unless $ltr2zobj{'s'}{'check'}{'exists'}->($svcid);
 my $svcName=$ltr2zobj{'s'}{'name'}{'get'}->($svcid);
 my $ltrs=join(''=>keys %ltr2zobj);
 $svcName=~s%\s*\([${ltrs}]\d{1,10}\)$%%;
 $svcName.=' ('.$zobjid.')';
 $ltr2zobj{'s'}{'name'}{'update'}->($svcid,$svcName);
}

sub doDeassocITService {
 my $svcid=shift;
 return {'error'=>'No such ITService'} unless my $svcName=$ltr2zobj{'s'}{'name'}{'get'}->($svcid);
 return {'result'=>($svcName=~s%${rxZOSfx}%%?$ltr2zobj{'s'}{'name'}{'update'}->($svcid,$svcName):-1)};
}

sub getServiceIDsByNames {
 return undef unless @_;
 map {
  if (/[^\d]/) {
   (my $st=$sql_{'getSvcByName'}{'st'})->execute($_);
   my @r=@{$st->fetchall_arrayref({})};
   return undef if @r>1 or !@r;
   $r[0]{'serviceid'}+0;
  } else {
   $_+0
  }
 } @_;
}

sub chkZObjExists {
 my $zobjid=shift;
 my $ltrs=join(''=>keys %ltr2zobj);
 return () unless my ($objType,$objID)=$zobjid=~m/^([${ltrs}])(\d{1,10})$/;
 return $ltr2zobj{$objType}{'check'}{'exists'}->($objID)?($objType,$objID):();
}

sub getITSvcChildrenByName { 
 my ($parSvcID, $svcName) = @_;
 my $st = $sql_{($parSvcID ? 'getSvcByNameAndParent' : 'getSvcByNameUnderRoot') . ($svcName=~$rxZOSfx ? '' : 'RX')}{'st'};
 $st->execute($svcName, $parSvcID ? ($parSvcID) :() );
 return $st->fetchall_arrayref({});
}

sub chkITServiceBy_ID_Name_Parent {
 my ($serviceid, $name, $parentid)=@_;
# say Dumper [@sql_{map 'chkSvcIDName'.$_, 'Parent', 'UnderRoot'}];
 my $st=$sql_{'chkSvcIDName'.($parentid?'Parent':'UnderRoot')}{'st'};
 $st->execute($serviceid, $name, $parentid?($parentid):());
 return $st->fetchall_arrayref()->[0][0];
}

sub genITServicesTree {
 my ($svc,$parSvcId,$svcName)=@_;
 confess 'Service must be presented as a hash reference' unless ref $svc eq 'HASH';
 $svcName||=$svc->{'name'};
 $parSvcId||=$svc->{'parentid'};
 $svc->{'genstat'}=_E('Cant create or link unnamed service without serviceid'), return
  unless $svc->{'serviceid'} or (defined($svcName) and length($svcName));
 $svc->{'genstat'}=_E('Cant link service to unknown parent'), return
  unless defined $parSvcId;
 # If serviceid is defined - check, whether this serviceid with the specified name is a child of appropr parent. 
 # 	If not - check whether this serviceid already exists and if it is - link this serviceid to the specified parent
 if ( defined $svc->{'serviceid'} and !chkITServiceBy_ID_Name_Parent(@{$svc}{qw/serviceid name/}, $parSvcId) ) {
  if ( chkITServiceExists($svc->{'serviceid'}) ) {
   doLinkITService($svc->{'serviceid'},$parSvcId,1);
   $svc->{'genstat'}={'ok'=>'linked'}
  } else {
   undef $svc->{'serviceid'}
  }
 }
 my $svcNodes=
  defined($svc->{'nodes'})
   ? ref $svc->{'nodes'} eq 'HASH'
    ? {map {my @kv=each $svc->{'nodes'}; scalar($kv[1]{'name'} || $kv[0])=>$kv[1] } 1..keys($svc->{'nodes'})}
    : ref $svc->{'nodes'} eq 'ARRAY'
     ? {map {$_->{'name'}=>$_} @{$svc->{'nodes'}}}
     : {}
   : {};
 $svc->{'genstat'} = {'ok'=>'exists'}, return 1
  if defined $svc->{'serviceid'} and !%{$svcNodes};
 unless ( defined $svc->{'serviceid'} ) {
  my $flSameSvcDel;
  my @sameSvcs = @{getITSvcChildrenByName($parSvcId, $svcName)};
  if (exists $svc->{'recreate'} and @sameSvcs) {
   if ($svc->{'recreate'}) {
    $svc->{'genstat'} = _E('Cant recreate service: there are more than one services with the same name under same parent'), return 
     if $#sameSvcs;
    doDeleteITService2($sameSvcs[0]{'serviceid'});
    $flSameSvcDel = 1
   } else {
    $svc->{'serviceid'} = $sameSvcs[0]{'serviceid'};
    $svc->{'genstat'} = {'ok'=>'exists'};
    goto RECURSE_NODES;
   }
  }
  my %svcSettings=(
   'algorithm'	=>	SLA_ALGO_ALL_FOR_PROBLEM,
   'showsla'	=>	SHOW_SLA_CALC,
   'goodsla'	=>	DEFAULT_GOOD_SLA,
   'sortorder'	=>	0,
   'triggerid'	=>	undef,
  );
  # Hint: 'triggerid' is absent in %svcSettings, so we need to explicitly put it in @k
  my @k=grep exists $svcSettings{$_}, keys $svc;
  @svcSettings{@k}=@{$svc}{@k} if @k;
  my $res=doCreateITService({
    %svcSettings,
    'name'=>$svcName,
    'parentid'=>$parSvcId,
  });
  $svc->{'genstat'}=_E((ref $res eq 'HASH' and defined $res->{'error'})?$res->{'error'}:'UNKNOWN'), return
   if ref $res ne 'HASH' or defined $res->{'error'} or !defined($res->{'serviceids'});
  $svc->{'serviceid'}=$res->{'serviceids'}[0];
  $svc->{'genstat'}={'ok'=>$flSameSvcDel?'recreated':'created'}
 }

RECURSE_NODES:
 return 1 unless %{$svcNodes};
 my $parId=$svc->{'serviceid'};
# say Dumper $svcNodes;
 while ( my ($svcName,$svc)=each $svcNodes ) {
  genITServicesTree($svc,$parId,$svcName)
 }
 return $svc
}

sub getITServiceAPI {
 my ($svcParent,$serviceGetPars)=@_;
 my $childSvcs=zbx('service.get',{%{$serviceGetPars},'serviceids'=>$svcParent->{'serviceid'},'selectDependencies'=>['serviceid']});
 return undef unless ref($childSvcs) eq 'ARRAY' and @{$childSvcs};
 for my $refDep (map { map \$_, @{$_->{'dependencies'}} } grep {!$_->{'triggerid'} and @{$_->{'dependencies'}}} @{$childSvcs}) {
  $$refDep=getITServiceAPI($$refDep,$serviceGetPars);
  delete $$refDep->{'triggerid'} unless $$refDep->{'triggerid'};
 }
 return scalar($#{$childSvcs}?$childSvcs:$childSvcs->[0])
}

sub getITServiceIDByName {
 my @names=ref($_[0]) eq 'ARRAY'?@{$_[0]}:do { my @snh=split /\//,$_[0]; shift @snh if $snh[0] eq ''; @snh };
 my $parSvcID=$_[1];
# say "names=".join(','=>@names)." parsvc=".($parSvcID?$parSvcID:'<ROOT>');
 my $st=$sql_{'getSvcByName'.($parSvcID?'AndParent':'UnderRoot')}{'st'};
 $st->execute(scalar(shift @names),($parSvcID?$parSvcID:()));
 return undef unless my $svc=$st->fetchall_arrayref([])->[0];
 return @names?getITServiceIDByName(\@names,$svc->[0]):$svc->[0];
}

sub getITServicesAssociatedWith {
 my $zobjid=shift;
 $sql_{'getSvcByZOExt'}{'st'}->execute($zobjid);
 return $sql_{'getSvcByZOExt'}{'st'}->fetchall_arrayref({});
}

my %cacheSvcTree;
sub getITService {
 return undef unless $dbhGlob;
 my $svc=shift;
 unless (ref($svc)) {
  my $stGetSvc=$sql_{'getSvc'}{'st'};
  $stGetSvc->execute($svc);
  $svc=$stGetSvc->fetchall_arrayref({})->[0];
  return undef unless $svc;
  %cacheSvcTree=();
 }
 my $serviceid=$svc->{'serviceid'};
 return undef if $cacheSvcTree{$serviceid}{'rflag'};
 return $cacheSvcTree{$serviceid}{'obj'} if $cacheSvcTree{$serviceid}{'obj'};
 my ($zoType,$zoID)=zobjFromSvcName($svc->{'name'});
 $cacheSvcTree{$serviceid}{'rflag'}=1;
 utf8::decode($svc->{'name'});
 if ($svc->{'triggerid'}) {
  $svc->{'ztype'}='trigger';
  $svc->{'zobjid'}=$svc->{'triggerid'};
  my $stGetTrg=$sql_{'getTrg'}{'st'};
  $stGetTrg->execute($svc->{'triggerid'});
  my $trg=$stGetTrg->fetchall_arrayref({})->[0];
  $svc->{'lostfunk'}=($trg->{'priority'}-1)/4 if $trg->{'value'} and !$trg->{'status'} and $trg->{'priority'}>1;  
 } else {
  if ( my ($zoType,$zoID)=zobjFromSvcName(\$svc->{'name'}) ) {
   if ($zoType eq 't') {
    $svc->{'invalid'}=1;
    return $svc
   }
   if ( defined(my $zoDscrByType=$ltr2zobj{$zoType}) ) {
    $svc->{'ztype'}=$zoDscrByType->{'otype'};
    $svc->{'zobjid'}=$zoID;
    $svc->{$zoDscrByType->{'id_attr'}}=$zoID;
   }
  }
  delete $svc->{'triggerid'};
  if ($zoType eq 'h') {
   $sql_{'checkHostEnabled'}{'st'}->execute($zoID);
   unless (my $hostStatus=$sql_{'checkHostEnabled'}{'st'}->fetchall_arrayref([])->[0][0]) {
    $svc->{'unfinished'}=1;
    if (defined $hostStatus) {
     $svc->{'disabled'}=1
    } else {
     delete $svc->{'hostid'}
    }
   }
  }
  unless (exists $svc->{'disabled'}) {
   my $stGetDeps=$sql_{'getSvcDeps'}{'st'};
   $stGetDeps->execute($serviceid);
   if ( my @deps=grep { !exists $_->{'invalid'} } map { return undef unless my $t=getITService($_); $t } @{$stGetDeps->fetchall_arrayref({})} ) {
 #   @deps=@deps>1?iterate_as_array(\&getSvc,\@deps):(getSvc($deps[0]));
    if (my @ixTermDeps=grep { !exists $deps[$_]{'unfinished'} } 0..$#deps) {
     my $lostFunK=0;
     my $childLFKWeight=$svc->{'algorithm'}==SLA_ALGO_ALL_FOR_PROBLEM?(1/@ixTermDeps):1;
     $lostFunK+=$_*$childLFKWeight for grep $_, map $deps[$_]{'lostfunk'}, @ixTermDeps;
     $svc->{'lostfunk'}=$lostFunK>1?1:$lostFunK if $lostFunK;
    } else {
     $svc->{'unfinished'}=1;
    }
    $svc->{'dependencies'}=\@deps;
   } else {
    $svc->{'unfinished'}=1;
   }  
  }
 }
 $cacheSvcTree{$serviceid}{'rflag'}=0;
 $cacheSvcTree{$serviceid}{'obj'}=$svc;
}

sub getITService4jsTree {
 my ($svc,@pars)=@_;
 return unless $dbhGlob;
 unless (ref($svc)) {
  my $stGetSvc=$sql_{'getSvc'}{'st'};
  $stGetSvc->execute($svc);
  $svc=$stGetSvc->fetchall_arrayref({})->[0];
  return undef unless $svc;
  %cacheSvcTree=();
 }
 my $serviceid=$svc->{'serviceid'};
 return undef if $cacheSvcTree{$serviceid}{'rflag'};
 return $cacheSvcTree{$serviceid}{'obj'} if $cacheSvcTree{$serviceid}{'obj'};
 $cacheSvcTree{$serviceid}{'rflag'}=1; 
 utf8::decode($svc->{'name'});
 my ($zotype,$zoid);
 if ($svc->{'triggerid'}) {
  $zotype='t';
  $svc->{'ztype'}='trigger';
  $svc->{'zobjid'}=$zoid=$svc->{'triggerid'};
  my $stGetTrg=$sql_{'getTrg'}{'st'};
  $stGetTrg->execute($svc->{'triggerid'});
  my $trg=$stGetTrg->fetchall_arrayref({})->[0];
  $svc->{'lostfunk'}=($trg->{'priority'}-1)/4 if $trg->{'value'} and !$trg->{'status'} and $trg->{'priority'}>1;  
 } else {
  delete $svc->{'triggerid'};
  if ($svc->{'name'}=~s%(?:\s+|^)\(([a-zA-Z])(\d{1,10})\)$%% and defined $ltr2zobj{$1}) {
   ($zotype,$zoid)=($1,$2);
   $svc->{'ztype'}=$ltr2zobj{$zotype}{'otype'};
   $svc->{'zobjid'}=$zoid;
   $svc->{$ltr2zobj{$zotype}{'id_attr'}}=$zoid;
  }  
  my $stGetDeps=$sql_{'getSvcDeps'}{'st'};
  $stGetDeps->execute($serviceid);
# grep {! exists($_->{'unfinished'}) }
  if ( my @deps=map { return undef unless my $t=getITService4jsTree($_,@pars,$serviceid); $t } @{$stGetDeps->fetchall_arrayref({})} ) {
   my $lostFunK=0;
   my $childLFKWeight=$svc->{'algorithm'}==2?(1/@deps):1;
   $lostFunK+=$_*$childLFKWeight for grep $_, map $_->{'lostfunk'}, @deps;
   $svc->{'lostfunk'}=$lostFunK>1?1:$lostFunK if $lostFunK;
   $svc->{'children'}=\@deps;
  } else {
   $svc->{'unfinished'}=1;
  }
 }
 $svc->{'text'}=sprintf('%s [%d]',@{$svc}{'name','serviceid'});
 $svc->{'data'}={
  'algo'=>$svc->{'algorithm'},
 };
 if (defined $svc->{'ztype'}) {
  @{$svc->{'data'}}{map 'ZO_'.$_,qw(type id name)}=(ucfirst($svc->{'ztype'}), $zoid, $ltr2zobj{$zotype}{'name'}{'get'}->($zoid));
 }
 $svc->{'a_attr'}={
  'title'=>join('; ' => 
    sprintf('Service: id=%s algo=%s',@{$svc}{qw(serviceid algorithm)}),
    defined($svc->{'ztype'})?( 
     sprintf('%s: id=%s name=%s', @{$svc->{'data'}}{map 'ZO_'.$_,qw(type id name)})
    ):()
  ),
 };
 $svc->{'id'}=join '/' => @pars, $svc->{'serviceid'};
 $cacheSvcTree{$serviceid}{'rflag'}=0;
 $cacheSvcTree{$serviceid}{'obj'}=$svc; 
}

sub getITSCache {
 \%cacheSvcTree;
}

sub getAllITServiceDeps {
 sub svcDepsClean {
  my $svc=shift;
  return { map {$_=>$svc->{$_}} grep {$_ ne 'dependencies'} keys %{$svc} }
 }
 sub getDepsRecursive {
  my $svc=shift;
  return {} unless ref $svc;
  return svcDepsClean($svc) unless defined($svc->{'dependencies'}) and @{$svc->{'dependencies'}};
  return (
    svcDepsClean($svc),
    map getDepsRecursive($_), @{$svc->{'dependencies'}}
  )
 }
 getDepsRecursive(getITService(shift));
}

sub getITServiceDepsByType {
 my ($rootSvcID,$typeLetter)=@_;
 return {'error'=>'Wrong parameters passed'} 
  unless $ltr2zobj{$typeLetter} and $rootSvcID=~m/^\d{1,10}$/;
 return {'error'=>'Base ITService with the specified ID not found'}
  unless !$rootSvcID or chkITServiceExists($rootSvcID);
 my ($ztype,$idattr)=@{$ltr2zobj{$typeLetter}}{qw(otype id_attr)};
 return {'error'=>'You must properly initialize Zabbix API before passing base serviceid=0 to getITServiceDepsByType'} unless $rootSvcID or zbx_api_url;
 my @svcs=
  $rootSvcID
   ? grep {defined($_->{'ztype'}) and $_->{'ztype'} eq $ztype} getAllITServiceDeps($rootSvcID)
   : grep defined $_, map {
      $_->{'name'}=~s%^(.+)\s+\(${typeLetter}(\d+)\)$%$1%
       ? do {
          $_->{$idattr}=$2; $_
         }
       : undef
     } @{zbx('service.get', {'search'=>{'name'=>"*(${typeLetter}*)"},'output'=>['name']})};
}

1;
