#!/usr/bin/perl
use strict;
use warnings;
use 5.16.1;
use lib '/opt/Perl5/libs';
use Config::ShellStyle;
use DBI;
use Try::Tiny;
use Log4perl::KISS;
use constant {
  ZBX_API_CONFIG =>'/etc/zabbix/api/setenv_inframon.conf',
};

my $zbxEnv;
BEGIN {
  $zbxEnv=read_config(ZBX_API_CONFIG);
}

use Data::Dumper;
my %descrAssoc=(
 'host'=>{
  'id'=>'hostid',
  'tbl'=>'services_hosts',
  'pfx'=>'h',
 },
 'item'=>{
  'id'=>'itemid',
  'tbl'=>'services_items',
  'pfx'=>'i',
 },
 'hostgroup'=>{
  'id'=>'groupid',
  'tbl'=>'services_groups',
  'pfx'=>'g',
 },
 'trigger'=>{
  'id'=>'triggerid',
  'tbl'=>'services_triggers',
  'pfx'=>'t',
 },
 'application'=>{
  'id'=>'applicationid',
  'tbl'=>'services_applications',
  'pfx'=>'a',
 }, 
);

my $dbh=DBI->connect(sprintf('dbi:mysql:database=%s;host=%s',@{$zbxEnv}{map "DB_$_",qw/NAME HOST/}), @{$zbxEnv}{map "DB_$_",qw/USER PASS/}, {'RaiseError'=>1, 'mysql_enable_utf8'=>1});
my $what2do=shift // ($0=~m%(?:^|/)(\w+)-[^/]+$%)[0] // 'sync';
for ($what2do) {
 when (/^create/) {
  $dbh->begin_work;
  try {
   my $cntChanged;
   while (my ($assocClass, $assocAttrs)=each %descrAssoc) {
     __check_table_exists($dbh, $assocAttrs->{'tbl'}) 
       and
    warn_('Table %s already exists, skipping it', $assocAttrs->{'tbl'}), next;
    die 'Cant create table '.$assocAttrs->{'tbl'} 
     unless $dbh->do(sprintf <<'EOSQL', @{$assocAttrs}{qw/tbl id tbl id/});
CREATE TABLE IF NOT EXISTS %s (
 serviceid bigint(20) unsigned NOT NULL,
 %s bigint(20) unsigned NOT NULL,
 PRIMARY KEY (serviceid),
 KEY %ss_1 (serviceid,%s)
)
EOSQL
    $cntChanged++;
    my $zobjTbl=$assocAttrs->{'tbl'}=~s%^.+_%%r;
    die "Cant set on-delete-cascade constraint for $assocAttrs->{'tbl'}"
     unless $dbh->do(<<EOSQL);
ALTER
 TABLE $assocAttrs->{'tbl'}
 ADD
  CONSTRAINT c_$assocAttrs->{'tbl'}_1
   FOREIGN KEY ($assocAttrs->{'id'}) REFERENCES $zobjTbl ($assocAttrs->{'id'}) 
   ON DELETE CASCADE
EOSQL
   }
   debug_ $cntChanged ? $dbh->commit ? 'DONE' : die($dbh->errstr) : 'NO CHANGES';
  } catch {
   my $err=$_;
   $dbh->rollback;
   die "Error occured while creating associative tables: $err";
  };
 }
 when (/^sync/) {
  my %svcs;
  for my $svc ( @{$dbh->selectall_arrayref(q{SELECT `serviceid`, `name`, `triggerid` FROM services}, {'Slice'=>{}})} ) {
   if ($svc->{'triggerid'}) {
    $svcs{'t'}{$svc->{'serviceid'}}=$svc->{'triggerid'}
   } elsif ($svc->{'name'}=~/(?:^|\s)\(([hgai])(\d+)\)$/) {
    $svcs{$1}{$svc->{'serviceid'}}=$2
   }
  }
  while (my ($assocClass, $assocAttrs)=each %descrAssoc) {
   my ($assocLtr, $assocTbl, $assocId)=@{$assocAttrs}{qw(pfx tbl id)};
   $dbh->begin_work if $dbh->{'AutoCommit'};
   debug { 'Working on %s', $assocTbl };
   try {
    my (@svc2del_from_svc2id, @svc2del_from_ins_bulk);
    my %svc_assoc_by_id=map @{$_}, @{$dbh->selectall_arrayref(sprintf q|SELECT serviceid, %s FROM %s|, $assocId, $assocTbl)};
    my %svc_assoc_by_name=eval { %{$svcs{$assocLtr}} };
    debug_('Nothing to sync in %s', $assocTbl), return unless %svc_assoc_by_id or %svc_assoc_by_name;
 #   my %svc_assoc_by_name=
 #    map { $_->[0]=>scalar(($_->[1]=~m/\s\(${assocLtr}(\d+)\)$/)[0]) }
 #     do { $sthGetNameAssocSvcs->execute(); say Dumper(my $l=$sthGetNameAssocSvcs->fetchall_arrayref()); @{$l} };
    while (my ($serviceid, $zobjid)=each %svc_assoc_by_name) {
     next unless defined $svc_assoc_by_id{$serviceid};
     if ($svc_assoc_by_id{$serviceid} ne $zobjid) {
      push @svc2del_from_svc2id, $serviceid
     } else {
      push @svc2del_from_ins_bulk, $serviceid
     }
    }
    if (@svc2del_from_svc2id or @svc2del_from_ins_bulk) {
     delete @svc_assoc_by_id{@svc2del_from_svc2id,@svc2del_from_ins_bulk};
     if (@svc2del_from_svc2id or %svc_assoc_by_id) {
      $dbh->do(my $sqlDelFromAssocTbl=sprintf(<<'EOSQL', $assocTbl, join(','=>@svc2del_from_svc2id, keys %svc_assoc_by_id)));
DELETE FROM %s WHERE serviceid IN (%s)
EOSQL
      debug { 'SQL: %s', $sqlDelFromAssocTbl };
     }
     delete @svc_assoc_by_name{@svc2del_from_ins_bulk};
    }
    unless (%svc_assoc_by_name) {
     debug { 'Nothing to insert to %s', $assocTbl };
     return
    }
    my $tmpTbl="tmp_${assocTbl}";
    my $zobjTbl=$assocTbl=~s%^.+_%%r;
    my $newAssocs=join(','=>map '('.join(','=>@{$_}).')', map [each %svc_assoc_by_name], 1..keys(%svc_assoc_by_name));
    do { debug_('SQL: %s', $_); die $dbh->errstr unless $dbh->do($_) } for split /;\s*(?:\n|$)/, <<EOSQL;
DROP TABLE IF EXISTS ${tmpTbl};
CREATE TABLE ${tmpTbl} LIKE ${assocTbl};
INSERT INTO ${tmpTbl} VALUES ${newAssocs};
UPDATE services s INNER JOIN ${tmpTbl} a USING(serviceid) LEFT JOIN ${zobjTbl} z ON z.${assocId}=a.${assocId} SET s.name=SUBSTR(s.name, 1, INSTR(s.name, CONCAT(' (${assocLtr}',a.${assocId},')'))-1) WHERE z.${assocId} IS NULL;
DELETE ${tmpTbl} FROM ${tmpTbl} LEFT JOIN ${zobjTbl} z USING(${assocId}) WHERE z.${assocId} IS NULL;
INSERT INTO ${assocTbl} SELECT * FROM ${tmpTbl};
DROP TABLE ${tmpTbl};
EOSQL
    say STDERR "$assocTbl synced" if $dbh->commit;
   } catch {
    my $err=$_;
    $dbh->rollback;
    die "Error occured while syncing associative table <<$assocTbl>>: $err";
   } finally {
    1;
   }; # <- try/catch/finally
  }# <- while hosts, items, trigers, ...
 }
}

sub __check_table_exists {
 my ($dbh,$tableName)=@_;
 $dbh->selectall_arrayref(qq(select count(1) from information_schema.tables where table_name='${tableName}' and table_schema=database()))->[0][0];
}

END {
 if ($dbh) {
  $dbh->disconnect;
#  $dbh->close
 }
}
