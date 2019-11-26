#!/usr/bin/perl
use strict;
use warnings;
use 5.16.1;
use lib '/opt/Perl5/libs';
use Try::Tiny;
use Log::Log4perl::KISS;
use Getopt::Long::Descriptive;
use ZAPI;
use DBR;

use Data::Dumper;
my %descrAssoc = (
  'host' => {
    'id'  => 'hostid',
    'tbl' => 'services_hosts',
    'pfx' => 'h',
  },
  'item' => {
    'id'  => 'itemid',
    'tbl' => 'services_items',
    'pfx' => 'i',
  },
  'hostgroup' => {
    'id'  => 'groupid',
    'tbl' => 'services_groups',
    'pfx' => 'g',
  },
  'trigger' => {
    'id'  => 'triggerid',
    'tbl' => 'services_triggers',
    'pfx' => 't',
  },
  'application' => {
    'id'  => 'applicationid',
    'tbl' => 'services_applications',
    'pfx' => 'a',
  },
);

my ($opts, $usage) = describe_options(
  '%c %o [create|sync]',
  ['clean|r', 'clean initialisation: remove all existing associative tables (usable only in accordance with "create" method)'],
  [],
  ['help|h', 'show this useful message']
);

if ($opts->help) {
  say $usage->text;
  exit 0
}
my $what2do    = shift // ( $0 =~ m%(?:^|/)(\w+)-[^/]+$% )[0] // 'sync';
my $flCleanInit = $opts->clean;

my $zapi       	= ZAPI->new;
my $dbh        	= $zapi->ldbh;
my $dbr 	= DBR->new($zapi->dbh);

for ($what2do) {
  when (/^create/) {
    $dbh->begin_work;
    try {
      my $cntChanged;
      while ( my ( $assocClass, $assocAttrs ) = each %descrAssoc ) {
        my $assocTbl = $assocAttrs->{'tbl'};
        unless ( $flCleanInit ) {
          print Dumper [$dbr->named_query('table_exists', 'subst' => {'table_name' => $assocTbl})];
          my ($res) = $dbr->named_query('table_exists', 'subst' => {'table_name' => $assocTbl});
          $res->[0]{'count'}
            and warn_( 'Table %s already exists, skipping it', $assocTbl ), next;
        } else {
          $dbr->named_query('drop_table', 'subst' => {'table_name' => $assocTbl})
        }
        
        $dbr->named_query('create_table', 'subst' => {'table_name' => $assocTbl, 'zobjid_name' => $assocAttrs->{'id'}, 'services_table_name' => 'services'});
        
        $cntChanged++;
        
        my $zobjTbl = ${$zapi->fixed_table_name( $assocTbl =~ s%^.+_%%r )};
      }
      debug_ $cntChanged ? $dbh->commit ? 'DONE' : die( 'ERROR: ' . $dbh->errstr ) : 'NO CHANGES NEEDED TO BE DONE';
    }
    catch {
      my $err = $_;
      $dbh->rollback;
      die "Error occured while creating associative tables: $err";
    };
  }
  when (/^sync/) {
    $dbr->add_named_query('remove_assocs_from_services' => {
      'postgresql' => <<'EOSQL',
UPDATE services
SET name=SUBSTR(name, 1, POSITION(CONCAT(' ({{assoc_ltr}}', t.a_{{assoc_id}}, ')') IN name) - 1)
FROM
  (SELECT {{temp_table}}.serviceid, {{temp_table}}.{{assoc_id}} a_{{assoc_id}}, {{zobj_table}}.{{assoc_id}} z_{{assoc_id}} FROM {{temp_table}} LEFT JOIN {{zobj_table}} USING({{assoc_id}})) t
WHERE t.serviceid=services.serviceid AND t.z_{{assoc_id}} IS NULL
EOSQL
      'mysql'	=> <<'EOSQL',
UPDATE 
  services s
    INNER JOIN {{temp_table}} a USING(serviceid)
    LEFT JOIN {{zobj_table}} z ON z.{{assoc_id}}=a.{{assoc_id}}
SET s.name=SUBSTR(s.name, 1, INSTR(s.name, CONCAT(' ({{assoc_ltr}}', a.{{assoc_id}}, ')')) - 1)
WHERE z.{{assoc_id}} IS NULL
EOSQL
    });
    $dbr->add_named_query('delete_from_tmp_table' => {
      'postgresql' =>  <<'EOSQL',
DELETE FROM ONLY {{temp_table}} USING (SELECT t.serviceid, z.{{assoc_id}} FROM {{temp_table}} t LEFT JOIN {{zobj_table}} z USING({{assoc_id}})) tt WHERE tt.serviceid={{temp_table}}.serviceid AND tt.{{assoc_id}} IS NULL
EOSQL
      'mysql'	=> <<'EOSQL',
DELETE {{temp_table}} FROM {{temp_table}} LEFT JOIN {{zobj_table}} z USING({{assoc_id}}) WHERE z.{{assoc_id}} IS NULL
EOSQL
    });
    my %svcs;
    for my $svc ( @{ $dbh->selectall_arrayref( q{SELECT serviceid, name, triggerid FROM services}, { 'Slice' => {} } ) } )
    {
      if ( $svc->{'triggerid'} ) {
        $svcs{'t'}{ $svc->{'serviceid'} } = $svc->{'triggerid'};
      } elsif ( $svc->{'name'} =~ /(?:^|\s)\(([hgai])(\d+)\)$/ ) {
        $svcs{$1}{ $svc->{'serviceid'} } = $2;
      }
    }
    while ( my ( $assocClass, $assocAttrs ) = each %descrAssoc ) {
      my ( $assocLtr, $assocTbl, $assocId ) = @{$assocAttrs}{qw(pfx tbl id)};
      my $flErrorCatched;
      $dbh->begin_work if $dbh->{'AutoCommit'};
      debug { 'Working on %s', $assocTbl };
      try {
        my ( @svc2del_from_svc2id, @svc2del_from_ins_bulk );
        my %svc_assoc_by_id = map @{$_},
          @{ $dbh->selectall_arrayref( sprintf q|SELECT serviceid, %s FROM %s|, $assocId, $assocTbl ) };
        my %svc_assoc_by_name = eval { %{ $svcs{$assocLtr} } };
        debug_( 'Nothing to sync in %s', $assocTbl ), return unless %svc_assoc_by_id or %svc_assoc_by_name;

        #   my %svc_assoc_by_name=
        #    map { $_->[0]=>scalar(($_->[1]=~m/\s\(${assocLtr}(\d+)\)$/)[0]) }
        #     do { $sthGetNameAssocSvcs->execute(); say Dumper(my $l=$sthGetNameAssocSvcs->fetchall_arrayref()); @{$l} };
        while ( my ( $serviceid, $zobjid ) = each %svc_assoc_by_name ) {
          next unless defined $svc_assoc_by_id{$serviceid};
          if ( $svc_assoc_by_id{$serviceid} ne $zobjid ) {
            push @svc2del_from_svc2id, $serviceid;
          } else {
            push @svc2del_from_ins_bulk, $serviceid;
          }
        }
        if ( @svc2del_from_svc2id or @svc2del_from_ins_bulk ) {
          delete @svc_assoc_by_id{ @svc2del_from_svc2id, @svc2del_from_ins_bulk };
          if ( @svc2del_from_svc2id or %svc_assoc_by_id ) {
            $dbh->do( my $sqlDelFromAssocTbl =
                sprintf( <<'EOSQL', $assocTbl, join( ',' => @svc2del_from_svc2id, keys %svc_assoc_by_id ) ) );
DELETE FROM %s WHERE serviceid IN (%s)
EOSQL
            debug { 'SQL: %s', $sqlDelFromAssocTbl };
          }
          delete @svc_assoc_by_name{@svc2del_from_ins_bulk};
        }
        unless (%svc_assoc_by_name) {
          debug { 'Nothing to insert to %s', $assocTbl };
          return;
        }
        my $tmpTbl  = "tmp_${assocTbl}";
        my $zobjTbl = ${$zapi->fixed_table_name( $assocTbl =~ s%^.+_%%r )};
        my $newAssocs =
          join( ',' => map '(' . join( ',' => @{$_} ) . ')', map [ each %svc_assoc_by_name ], 1 .. keys(%svc_assoc_by_name) );
#        say '1111';
        $dbr->named_query('drop_table', subst => {'table_name' => $tmpTbl});
        $dbr->named_query('create_table_like', subst => {'new_table_name' => $tmpTbl, 'like_existing' => $assocTbl});
        $dbh->do("INSERT INTO ${tmpTbl} VALUES ${newAssocs}");
        my $subst = +{'temp_table' => $tmpTbl, 'zobj_table' => $zobjTbl, 'assoc_ltr' => $assocLtr, 'assoc_id' => $assocId};        
        $dbr->named_query('remove_assocs_from_services', subst => $subst);
        $dbr->named_query('delete_from_tmp_table', subst => $subst);
#        say '2222';
        $dbh->do($_) for split /;\s*(?:\r?\n\s*)+/, <<EOSQL;
INSERT INTO ${assocTbl} SELECT * FROM ${tmpTbl};
DROP TABLE ${tmpTbl};
EOSQL
#        say '3333';
#        $dbh->commit ? info_ "$assocTbl synced" : logdie_('failed to sync %s: %s', $assocTbl, $dbh->errstr);
      }
      catch {
        my $err = $_;
        $flErrorCatched = 1;
        $dbh->rollback;
        die "Error occured while syncing associative table <<$assocTbl>>: $err";
      }
      finally {
        unless ( $flErrorCatched ) {
          $dbh->commit
            ? info_ "$assocTbl synced"
            : logdie_('failed to sync %s: %s', $assocTbl, $dbh->errstr);
        }
        1;
      };    # <- try/catch/finally
    }    # <- while hosts, items, trigers, ...
  }
}
