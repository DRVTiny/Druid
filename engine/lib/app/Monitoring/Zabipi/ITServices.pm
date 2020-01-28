package Monitoring::Zabipi::ITServices;
use Monitoring::Zabipi qw(zbx zbx_get_dbhandle zbx_api_url zbx_last_err);

use v5.16.1;
use utf8;
use boolean;
binmode $_, ':utf8' for *STDOUT, *STDERR;
sub _E { { 'error' => @_ >= 2 ? sprintf( $_[0], @_[ 1 .. $#_ ] ) : $_[0] } }
our $VERSION = 1.112;
use constant {
    SLA_ALGO_DO_NOT_CALC     	=> 0,
    SLA_ALGO_ONE_FOR_PROBLEM 	=> 1,
    SLA_ALGO_ALL_FOR_PROBLEM 	=> 2,
    SHOW_SLA_DO_NOT_CALC     	=> 0,
    SHOW_SLA_CALC            	=> 1,
    DFLT_GOOD_SLA            	=> '99.05',
    IFACE_TYPE_ZABBIX_AGENT  	=> 1,
    IFACE_TYPE_SNMP          	=> 2,
    HARD_LINK                	=> 0,
    SOFT_LINK			=> 1,
    DEF_INTEGER_AS_STRING    	=> 4352,
    DEF_POSITIVE_INTEGER     	=> 1,
    DFLT_ROOT_SERVICEID      	=> 0,
    DFLT_NAME_FIELDS_SEP	=> ' ',
    DFLT_ROOT_NAME		=> '** ROOT **',
    DFLT_FETCH_METHOD	     	=> 'fetchall_arrayref',
    YES				=> 1,
    NO				=> undef,
    AS_HASH_REF			=> 1,
    PARENTID			=> 0,
};
use Ref::Util qw(is_hashref is_arrayref is_scalarref is_plain_hashref is_plain_arrayref is_plain_scalarref);
use ZAPI;
use DBR;
my %ltr2zobj = (
    'i' => {
        'otype'   => 'item',
        'id_attr' => 'itemid',
        'table'   => 'items',
        'name'    => { 'attr' => 'name' },
    },
    's' => {
        'otype'   => 'service',
        'id_attr' => 'serviceid',
        'table'   => 'services',
        'name'    => { 'attr' => 'name' },
        'zo_attrs' => ['triggerid'],
    },
    't' => {
        'otype'   => 'trigger',
        'id_attr' => 'triggerid',
        'table'   => 'triggers',
        'name'    => { 'attr' => 'description' },
    },
    'h' => {
        'otype'   => 'host',
        'id_attr' => 'hostid',
        'table'   => 'hosts',
        'name'    => { 'attr' => [qw(host name)], 'fmt' => '%s (%s)' },
    },
    'g' => {
        'otype'   => 'hostgroup',
        'id_attr' => 'groupid',
        'table'   => '{{__zo_hostgroups_table}}',
        'name'    => { 'attr' => 'name' },
    },
    'a' => {
        'otype'   => 'application',
        'id_attr' => 'applicationid',
        'table'   => 'applications',
        'name'    => { 'attr' => 'name' },
    },
    'u' => {
        'otype'   => 'user',
        'id_attr' => 'userid',
        'table'   => 'users',
        'name' => { 'attr' => [qw(alias name surname)], 'fmt' => '%s (%s %s)' },
    },
    'U' => {
        'otype'   => 'usergroup',
        'id_attr' => 'usergroupid',
        'table'   => 'usrgrp',
        'name'    => { 'attr' => 'name' },
    },
    'm' => {
        'otype'   => 'mediatype',
        'id_attr' => 'mediatypeid',
        'table'   => 'media_type',
        'name'    => { 'attr' => 'description' },
    },
    'M' => {
        'otype'   => 'media',
        'id_attr' => 'mediaid',
        'table'   => 'media',
        'name'    => { 'attr' => 'sendto' },
    },
);
our $rxZOPfxs = join '' => keys %ltr2zobj;
my $rxZOSfx = qr/\s*(\(([${rxZOPfxs}])(\d{1,10})\))$/;

use Exporter qw(import);
our @EXPORT_OK =
  qw(doDeleteITService doDeleteLeafITService genITServicesTree getITService getAllITServiceDeps doMoveITService getServiceIDsByNames doSymLinkITService chkZObjExists doAssocITService getITServicesAssociatedWith doCreateITService getITSvcChildrenByName);
our @EXPORT =
  qw($rxZOPfxs doDeleteITService doDeleteLeafITService doMoveITService doRenameITService getITService getITService4jsTree genITServicesTree getServiceIDsByNames doSymLinkITService doUnlinkITService getITSCache setAlgoITService chkZObjExists doAssocITService doDeassocITService getITServiceDepsByType doITServiceAddZOAttrs zobjFromSvcName getITServicesAssociatedWith getITServiceIDByName doCreateITService );

use DBI;
use Data::Dumper;
use Carp qw(confess);
use Ref::Util qw(is_plain_hashref is_plain_arrayref is_hashref is_arrayref);
use Scalar::Util qw(blessed);
use Scalar::Util::LooksLikeNumber qw(looks_like_number);
use Try::Tiny;

use subs qw/__is_pos_integer _E/;

#my @itsvcAttrs=qw(serviceid name algorithm triggerid showsla);
my %validSvcAttrs = (
    'serviceid' => undef,
    'name'      => undef,
    'algorithm' => undef,
    'sortorder' => undef,
    'showsla'   => undef,
    'goodsla'   => 99.9,
    'status'    => 0,
    'triggerid' => undef,
);
my @legalSvcAttrs = keys %validSvcAttrs;
my @mandSvcAttrs  = qw(serviceid name algorithm sortorder showsla);

sub getSQLSvcAttrs {
    if ( my $tbl = shift ) {
        return join( ',' => map $tbl . '.' . $_, @legalSvcAttrs );
    } else {
        return join( ',' => @legalSvcAttrs );
    }
}

my %sql_ = (
    'addSvc' => {
        'rq' => [
            'INSERT INTO services (%s) VALUES (%s)',
            join( ',' => @legalSvcAttrs ),
            join( ',' => ('?') x scalar(@legalSvcAttrs) )
        ],
    },
    'addSvcLink' => {
        'rq' => qq<INSERT INTO services_links (linkid,servicedownid,serviceupid,soft) VALUES (?,?,?,?)>,
    },
    'delLeafSvc' => {
        'rq' => 'delete from services where serviceid=? and not serviceid in (select serviceupid from services_links where soft=0)',
    },
    'getSvcDeps' => {
        'rq' => [
qq(select %s from services s inner join services_links l on s.serviceid=l.servicedownid where l.serviceupid=?),
            getSQLSvcAttrs('s')
        ]
    },
    'getAllSvcAssocWithZType' => {
        'rq' => q<SELECT name, serviceid FROM services WHERE {{__dbr_regexp(name ::: CONCAT('\(', ?, '[[:digit:]][[:digit:]]*\)$'))}}>
    },
    'getSvcHdDepsShort' => {
        'rq' =>
qq(select s.serviceid from services s inner join services_links l on s.serviceid=l.servicedownid where l.serviceupid=? and l.soft=0),
    },
    'getSvc' => {
        'rq' =>
          [ qq(select %s from services where serviceid=?), getSQLSvcAttrs() ]
    },
    'chkSvcIDNameParent' => {
        'rq' => qq(
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
            )
    },
    'chkSvcIDNameUnderRoot' => {
        'rq' => qq(
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
            )
    },
    'getSvcByZOExt' => {
        'rq' => [
            qq(select %s from services where name like concat('% (',?,')')),
            getSQLSvcAttrs()
        ]
    },
    'getSvcChildren' => {
        'rq' => [
qq(select %s from services_links l inner join services c on l.servicedownid=c.serviceid and l.serviceupid=?),
            getSQLSvcAttrs('c')
        ]
    },
    'getNameById' => {'rq' => 'select name from services where serviceid=?', 'single_result' => 1},
    'getHardParents' => { 'rq' => <<'EOSQL' },
select servicedownid "serviceid", serviceupid "parentid" from services_links where servicedownid {{=child_serviceid}} and soft=0
    union
select distinct ss.serviceid "serviceid", 0 "parentid" from services ss left join (select servicedownid serviceid from services_links where soft=0) sd on sd.serviceid=ss.serviceid where ss.serviceid {{=child_serviceid}} and sd.serviceid is null
EOSQL
    'getSoftParents' => { 'rq' => 'select serviceupid from services_links where soft>0 and servicedownid=?' },
    'getAllParents'  => { 'rq' => <<'EOSQL' },
select parentid, soft from
(select serviceupid parentid, soft from services_links where servicedownid={{child_serviceid}}
    union
select distinct 0 parentid, 0 soft from services ss left join (select servicedownid serviceid from services_links where soft=0) sd on sd.serviceid=ss.serviceid where ss.serviceid={{child_serviceid}} and sd.serviceid is null) tp
order by "soft"
EOSQL
    'getSvcHardDeps' => {
        'rq' =>
'select s.serviceid from services s inner join services_links l on l.servicedownid=s.serviceid and l.soft=0 where l.serviceupid=?'
    },
    'getRootSvcChildren' => {
        'rq' => [
qq(select %s from services s left outer join services_links l on l.servicedownid=s.serviceid and l.soft=0 where l.servicedownid is null),
            getSQLSvcAttrs('s')
        ]
    },
    'isSvcExists?' => { 'rq' => 'select count(*) from services where serviceid=?', 'single_result' => 1 },
    'isRootSvc?' => {
        'rq' => qq(select serviceupid from services_links where servicedownid=?)
    },
    'getTrg' => {
        'rq' =>
          qq(select priority,value,status from triggers where triggerid=?),
    },
    'move2OtherParent' => {
        'rq' =>
          qq<update services_links set serviceupid={{new_parent_serviceid}} where serviceupid={{old_parent_serviceid}} and servicedownid={{child_serviceid}}>,
    },
    'getSvcByName' =>
      { 'rq' => qq(select serviceid from services where name=?), },
    'getSvcByNameAndParentRX' => {
        'rq' =>
qq(select s.serviceid from services s inner join services_links sl on s.serviceid=sl.servicedownid where s.name regexp concat('^',?,'( \\\\([${rxZOPfxs}][0-9]+\\\\))?\$') and sl.serviceupid=?      ),
    },
    'getSvcByNameUnderRootRX' => {
        'rq' =>
qq(select s.serviceid from services s left  join services_links sl on s.serviceid=sl.servicedownid where s.name regexp concat('^',?,'( \\\\([${rxZOPfxs}][0-9]+\\\\))?\$') and sl.serviceupid is null),
    },
    'getSvcByNameAndParent' => {
        'rq' => <<'EOSQL',
SELECT
  s.serviceid
FROM 
  services s
    LEFT JOIN services_links sl ON s.serviceid=sl.servicedownid
WHERE
  {{__dbr_regexp(s.name ::: CONCAT('^', ?, '([[:space:]][[:space:]]*\([hgt][[:digit:]][[:digit:]]*\))?$'))}}
    AND
  sl.serviceupid {{=parentid}}
EOSQL
    },
    'getSvcByNameUnderRoot' => {
        'rq' =>
qq(select s.serviceid from services s left  join services_links sl on s.serviceid=sl.servicedownid where {{__dbr_regexp(s.name ::: CONCAT(?, ' \([hgt][[:digit:]][[:digit:]]*\)$'))}} and sl.serviceupid is null),
    },
    'renSvcByName' => { 'rq' => qq(update services set name=? where name=?), },
    'renSvcByID' =>
      { 'rq' => qq(update services set name=? where serviceid=?), },
    'unlinkSvc' => {
        'rq' =>
qq(delete from services_links where serviceupid=? and servicedownid=? and soft>0),
    },
    'moveUnderRoot' => {
        'rq' => 'delete from services_links where servicedownid=? and soft=0'
    },
    'algochgSvc' =>
      { 'rq' => qq(update services set algorithm=? where serviceid=?) },
    'checkHostEnabled' => {
        'rq' =>
            qq(select {{__dbr_iif(maintenance_status=1 or status=1 ::: 0 ::: 1)}} flHostMonStatus from hosts where hostid=?),
        'single_result' => 1,
    },

);

my %idOps = (
    'services' => {
        'id_attr' => 'serviceid'
    },
    'services_links' => {
        'id_attr' => 'linkid'
    },
);

sub new {
    state $zoMethods = {
        'name.get' => {
            'attrs2select' => sub {
                __turn2list($_[0]{'name'}{'attr'})
#                is_plain_arrayref($_[0]{'name'}{'attr'}) ? @{$_[0]{'name'}{'attr'}} : ($_[0]{'name'}{'attr'})
            },
            'generator' => sub {
                my ($sth, $zo) = @_;
                sub {
                    $sth->execute( my $oid = shift );
                    my @res = map { utf8::decode($_); $_ } @{$sth->fetchall_arrayref([])->[0]};
                    return
                    $zo->{'name'}{'fmt'}
                        ? sprintf( $zo->{'name'}{'fmt'}, @res )
                        : join( DFLT_NAME_FIELDS_SEP() => @res )
                } 
            },
        },
        'zobj.get' => {
            'attrs2select' => sub { ( $_[0]{'id_attr'}, __turn2list($_[0]{'name'}{'attr'}), exists($_[0]{'zo_attrs'}) ? @{$_[0]{'zo_attrs'}} : () ) },
            'generator' => sub {
                my ($sth, $zo) = @_;
                sub {
                    $sth->execute( my $oid = shift );
                    my $zobj = $sth->fetchall_arrayref({})->[0]
                        or return;
                    my $zoNameAttrs = $zo->{'name'}{'attr'};
                    for ( is_arrayref($zoNameAttrs) ? @{$zoNameAttrs} : ($zoNameAttrs) ) {
                        utf8::decode( $zobj->{$_} ) unless utf8::is_utf8( $zobj->{$_} );
                    }
                    return $zobj
                }
            },
        },
        'exists?' => {
            'attrs2select' => 'count(*)',
            'generator' => sub {
                my ($sth, $zo) = @_;
                sub {
                    $sth->execute( my $oid = shift );
                    eval { $sth->fetchall_arrayref([])->[0][0] }
                }
            }
        },
          'name.update' => {
            'generator' => sub {
                my $sth = $_[0];
                sub {                    
                    my $oid = shift;
                    $sth->execute( @_, $oid );
                    $sth->rows
                }
            }
        },
    };
    my $class = shift;
    my $zapi = shift // ZAPI->new;
    my $dbr = DBR->new(my $dbh = shift // $zapi->ldbh // return { 'error' => 'You must specify dbh handler as a first parameter' });
    $dbr->set_fun_sep_args(qr/\s*:::\s*/);
    my %sq_reqs = %sql_;
    while (my ($sqlReqName, $sqlReqConf) = each %sq_reqs) {
        my $rq = $sqlReqConf->{'rq'};
        if (is_hashref($rq) or index($rq, '{{__dbr_') >= 0 or index($rq, '{{=') >= 0) {
# this is the database-engine-relative request
            $dbr->add_named_query($sqlReqName => $rq);
            $sqlReqConf->{'exec'} = sub {
                my %opts = @_;
                my ($r) = $dbr->exec_named_query($sqlReqName, delete( $opts{'as_hash_refs'} ) ? (method_args => [{}]) : $sqlReqConf->{'single_result'} ? (method_args => [[]]) : () , %opts);
                return $sqlReqConf->{'single_result'} ? $r->[0][0] : $r;
            }
        } else {
# simple request
            my $baseQuery = is_arrayref($rq) ? sprintf(shift(@{$rq}), @{$rq}) : $rq;
            my $flIsSelect = $baseQuery =~ m/^\s*(?i:SELECT|DESC|SHOW)/;
            $sqlReqConf->{'exec'} = sub {
                my %opts = @_;
                my $subst = $opts{'subst'} // {};
                my $sth = $dbh->prepare( $baseQuery =~ s<\{\{([a-zA-Z_][a-zA-Z_0-9]*)\}\}>[$subst->{$1} // 'NULL']grex );
                my @method_args = delete( $opts{'as_hash_refs'} ) ? ({}) : ();
                my $rv = $sth->execute(@{$opts{'binds'} // []});
                if ( $flIsSelect ) {
                    my $method = $opts{'method'} // DFLT_FETCH_METHOD;
                    my $r = $sth->$method(@method_args);
                    return $sqlReqConf->{'single_result'} ? $r->[0][0] : $r;
                } else {
                    $rv
                }
            }
        }
    }
    my $self = bless
        +{
            zapi 	=> $zapi,
            dbr		=> $dbr,
            sql 	=> \%sq_reqs,
            zo  	=> my $zobjs = {},
        },
        ref($class) || $class;
    while ( my ($zoltr, $zo) = each %ltr2zobj ) {
        for my $method ( 'name.get', 'name.update', 'zobj.get', 'exists?' ) {
            my $attrs2select = $zoMethods->{$method}{'attrs2select'};
            (my $query = __sprintf(do {
              if ( $method =~ /\.update$/ ) {
                my $zoNameAttrs = $zo->{'name'}{'attr'};
                [
                    'UPDATE %s SET %s WHERE %s=?',
                    $zo->{'table'},
                    join(',' => map $_ . '=?', is_plain_arrayref($zoNameAttrs) ? @{$zoNameAttrs} : $zoNameAttrs),
                    $zo->{'id_attr'}
                ]
              } else {
                [
                    'SELECT %s FROM %s WHERE %s=?',
                    join(','  =>
                    (
                        {
                            'ARRAY' => sub { @{$_[0]} 	},
                            'CODE' 	=> sub { $_[0]->($zo) 	},
                            ''	=> sub { ( $_[0] ) },
                        }->{ref $attrs2select}
                            //
                        sub { die 'attrs2select in zoMethods.' . $method . ' for zoltr=<<' . $zoltr . '>> belongs to invalid reftype ' . (ref($attrs2select) || 'SCALAR') }
                    )->( $attrs2select )),
                    @{$zo}{ 'table', 'id_attr' }
                ]
              }
            })) =~ s/\{\{(__zo_[^}]+)\}\}/$self->$1($zoltr)/gex;
            my $sth = $dbh->prepare($query);
            $zobjs->{$zoltr}{$method} = $zoMethods->{$method}{'generator'}->($sth, $zo);
         }
    }
#        $zo->{'name'}{'update'} = sub {
#            my ( $objid, $newName ) = @_;
#            my $stRename = $dbh->prepare(
#                sprintf(
#                    'UPDATE %s SET %s=? WHERE %s=?',
#                    $zo->{'table'},
#                    (
#                        ref( $zo->{'name'}{'attr'} )
#                        ? $zo->{'name'}{'attr'}[0]
#                        : $zo->{'name'}{'attr'}
#                    ),
#                    $zo->{'id_attr'}
#                )
#            );
#            $stRename->execute( $newName, $objid );
#        };
#        my $st = $dbh->prepare(
#            sprintf( 'SELECT 1 FROM %s WHERE %s=?',
#                @{$zo}{ 'table', 'id_attr' } )
#        );
#        $zo->{'check'}{'exists'} = sub {
#            return undef unless $_[0] =~ /^\d{1,10}$/;
#            $st->execute(shift);
#            $st->fetchrow_array();
#        };
    return $self;
}

sub zapi { $_[0]{'zapi'} }

sub __zo_hostgroups_table {
    my $self = $_[0];
    ${$self->{'zapi'}->fixed_table_name('groups')};
}

sub __ldbh {
    $_[0]{'zapi'}->ldbh;
}

sub __query {
    my ($self, $queryName) = (shift, shift);
    &{$self->{'sql'}{$queryName}{'exec'}};
}

sub __nextid {
    my ($self, $what2do, $tableName) = @_;
    $what2do eq 'get' or $what2do eq 'incr' or confess 'dont know how to do this: ' . $what2do;
    my $dbhGlob = $self->{'zapi'}->ldbh;
    my $idAttr = $idOps{$tableName}{'id_attr'};
    my $lastId;
    for (my $c = 0; $c < 2; $c++) {
        last if
            $lastId = eval { $dbhGlob->selectall_arrayref(
                'SELECT nextid FROM ids WHERE table_name=? AND field_name=? FOR UPDATE',
                {}, 				$tableName,    $idAttr
            )->[0][0] };
            
        my $insQuery = sprintf(<<'EOSQL', $tableName, $idAttr, $idAttr, $tableName);
INSERT INTO ids (table_name,field_name,nextid) 
VALUES ('%s', '%s', (SELECT CASE COUNT(t.lastid) WHEN 0 THEN 1 ELSE MAX(t.lastid) END FROM (SELECT MAX("%s") lastid FROM %s) t GROUP BY t.lastid))
EOSQL
        try {
            $dbhGlob->do($insQuery);
        } catch {
            confess 'SQL QUERY FAILED: <<' . $insQuery . '>>. Reason provided by server: ' . $_;
       };
    }
    defined($lastId) or confess sprintf 'cant determine nextid for table <<%s>> and field <<%s>>', $tableName, $idAttr;
        
    $what2do eq 'get'
        ? ($lastId + 1)
        : $dbhGlob->do(
            q<UPDATE ids SET nextid=nextid+? WHERE table_name=? AND field_name=?>,
            {}, 		$_[3] // 1, 	   $tableName, 	    $idAttr
          )
}

sub __lastDBHError {
    $_[0]{'zapi'}->ldbh->errstr
}

sub link_it_here {
    my ( $self, $what, $where, $soft ) = @_;
    my $dbhGlob = $self->{'zapi'}->ldbh;
    my $svAutoCommit = $dbhGlob->{'AutoCommit'};
    $dbhGlob->{'AutoCommit'} = 0
      unless defined $svAutoCommit and $svAutoCommit == 0;
    $dbhGlob->do('START TRANSACTION');
    my $curLinkId = $self->__nextid('get' => 'services_links');
    $self->__nextid('incr' => 'services_links');
    $self->__query(
        'addSvcLink',
        binds => [$curLinkId, $what, $where, $soft ? 1 : 0]
    );
    $dbhGlob->commit;
    $dbhGlob->{'AutoCommit'} = $svAutoCommit if defined $svAutoCommit and !($svAutoCommit == $dbhGlob->{'AutoCommit'});
}

sub create {
    my ($self, $svcs) = @_;
    (ref($svcs)
        and (
            (is_plain_arrayref($svcs) and @{$svcs})
                or
            (is_plain_hashref($svcs)  and %{$svcs})
        )
    ) or return { 'error' => 'You must specify services to create' };
    $svcs = [$svcs] if is_plain_hashref($svcs);
    my $res;
    
    
    my $dbhGlob = $self->{'zapi'}->ldbh;
    my $saveAutoCommit = $dbhGlob->{'AutoCommit'};
    $dbhGlob->{'AutoCommit'} = 0;
    $dbhGlob->do('BEGIN');
    try {
        my $curSvcId = $self->__nextid('get' => 'services');

        # All... or nothing
        $self->__nextid('incr', 'services', scalar @{$svcs});
        my $curLinkId;
        if ( my $countLinks = grep defined( $_->{'parentid'} ), @{$svcs} ) {
            $curLinkId = $self->__nextid('get' => 'services_links');
            $self->__nextid('incr', 'services_links', $countLinks);
        }
        for my $svc ( @{$svcs} ) {
            my $parSvcId = $svc->{'parentid'};
            __filter_hash_ref($svc, \%validSvcAttrs);
            my $serviceid = $svc->{'serviceid'} = $curSvcId++;
            if ( my @attrsMiss = grep !defined( $svc->{$_} ), @mandSvcAttrs ) {
                die 'Failed to create service <<'
                  . ( $svc->{'name'} || 'UNKNOWN' )
                  . '>> : mandatory attribute'
                  . ( $#attrsMiss ? 's' : '' )
                  . ' missed: '
                  . join( ',' => @attrsMiss );
            }
            $self->__query(
                'addSvc',
                binds => [ map { defined( $svc->{$_} ) ? $svc->{$_} : $validSvcAttrs{$_} } @legalSvcAttrs ]
            );

            if ( $parSvcId ) {
                $self->__query(
                    'addSvcLink',
                    binds => [
                        $curLinkId++,
                        $serviceid,
                        $parSvcId,
                        HARD_LINK
                    ]
                ) or die sprintf 'Cant link <<%s>> to parent service #%d: database error %s', $svc->{'name'}, $parSvcId, $dbhGlob->errstr // 'UNKNOWN';
            }
        }
        $dbhGlob->commit
          or die 'Commit service changes failed: ' . $dbhGlob->errstr;
        $res = { 'serviceids' => [ map $_->{'serviceid'}, @{$svcs} ] }
    } catch {
        $res = { 'error' => $_ };
        $dbhGlob->rollback;
    } finally {
        $dbhGlob->{'AutoCommit'} = $saveAutoCommit
          unless $dbhGlob->{'AutoCommit'} == $saveAutoCommit;
    };
    return $res;
}

sub exists  {
    my ($self, $serviceid) = @_;
    __is_pos_integer( $serviceid ) or return;
    return YES if $serviceid == DFLT_ROOT_SERVICEID;
    $self->__query('isSvcExists?', binds => [ $serviceid ]);
}

# Returns:
#   [[parentid0, is_soft0], [parentid1, is_soft1]]
#    OR
#   {parentid0 => is_soft0, parentid1 => is_soft1}
# depending on 2nd parameter, $flAsAHash
sub get_svc_parents {
    my ( $self, $svcid, $flAsAHash ) = @_;
    return if $svcid == DFLT_ROOT_SERVICEID;
    
# @pars will be sorted by "soft" attribute
# \@pars format: [ [parentid0, soft0], [parentid1, soft1], ...]
    my @pars = @{$self->__query('getAllParents', subst => {child_serviceid => $svcid+0})}
      or die "cant get parents for service #${svcid}";
    $flAsAHash
      ? do {
          my $h = +{map @{$_}, @pars};
          wantarray ? %{$h} : $h
        }
      : wantarray
        ? @pars
        : \@pars;
}

sub get_children {
    my ( $self, $svcid, $flResolveZOName ) = @_;
    
    # Return if svcid defined, but contains anything "non-numeric"
    return if $svcid and ( ref($svcid) or $svcid =~ /[^\d]/ );
    my @svcChildren = 
    map {
        my $chldSvc = $_;
        utf8::is_utf8( $chldSvc->{'name'} )
          or utf8::decode( $chldSvc->{'name'} );
        $self->add_zo_attrs( $chldSvc, $flResolveZOName );
    } @{ $self->__query('get' . ( $svcid ? '' : 'Root' ) . 'SvcChildren', as_hash_refs => true, $svcid ? ('binds' => [$svcid]) : () ) };

    wantarray
      ? return (@svcChildren)
      : return ( \@svcChildren );
}

sub getITServiceHardDeps {
    my ($self, $serviceid) = @_;
    $serviceid or confess 'You must specify serviceid > 0';
    [ map $_->[0], @{ $self->__query('getSvcHdDepsShort', binds => [ $serviceid ]) } ]
}

sub deleteRecursively {
    my ( $self, $serviceid, %opts ) = @_;
    my $getHardDeps = $self->{'sql'}{'getSvcHdDepsShort'}{'exec'};

    sub getSvcDeps {
        my $svcid = shift;
        ( 
            map getSvcDeps( $_->[0] ), @{$self->__query('getSvcHdDepsShort', binds => [$svcid])}
        ),
          $svcid;
    }
    my @deps = getSvcDeps($serviceid);
    zbx( 'service.deletedependencies', \@deps );
    zbx( 'service.delete',             \@deps );
}

sub delete_leaf {
    my ($self, $svcid) = @_;
    $svcid or confess 'You must specify serviceid!';
    try {
        my $rv = $self->__query('delLeafSvc', binds => [$svcid]);
        #$dbhGlob->commit unless $dbhGlob->{'AutoCommit'};
        return $rv
          ? { 'serviceids' => [$svcid] }
          : { 'error' => 'No such LEAF it service #' . $svcid }
    } catch {
        return { 'error' => $_ }
    };
}

sub delete {
    my ($self, $serviceid) = @_;
    $serviceid or die 'You must pass serviceid of the it service to be removed. This serviceid cant be zero (root)!';
    my ( @delSvcs, $r );

    # Recursive delete all hard-dependencies (children)
    for my $childSvcId ( @{ $self->getITServiceHardDeps($serviceid) } ) {
        $r = $self->delete( $childSvcId );
        confess 'Failed to remove IT service #'
          . $childSvcId
          . ( is_hashref($r) ? ': ' . $r->{'error'} : '' )
          if ! ref($r) or (is_hashref($r) and defined($r->{'error'}));
        push @delSvcs, @{ $r->{'serviceids'} };
    }

    # Than, delete IT service itself
    $r = $self->delete_leaf($serviceid);
    confess 'Failed to remove IT service #'
      . $serviceid
      . ( is_hashref($r) ? ': ' . $r->{'error'} : '' ) . Dumper($r)
      if ! ref($r) or (is_hashref($r) and defined($r->{'error'}));
    return { 'serviceids' => [ @delSvcs, $serviceid ] };
}

sub getParents {
    my ($self, $serviceid, $flOnlyHard) = @_;
    die Dumper [$self->__query('getSoftParents', binds => [$serviceid])]; #subst => {child_serviceid => $serviceid})];
    map $_->{'parentid'}, ( 
        @{$self->__query('getHardParents', subst => {child_serviceid => $serviceid})},
        $flOnlyHard 
            ? () 
            : @{$self->__query('getSoftParents', binds => [$serviceid])}
    )
}

sub move {
    my ( $self, $what2mv, $where2place ) = @_;
    $what2mv or die 'cant move ROOT service anywhere';
    unless ( $where2place ) {
        return $self->__query('moveUnderRoot', binds => [$what2mv]);
    }
    my @parentids = map $_->[0], $self->get_svc_parents($what2mv);
    if ($parentids[0] == 0) {
    # insert new services_links row if our child services seated directly under "root"
        my $dbh = $self->__ldbh;
        my $saveAutoCommit = $dbh->{'AutoCommit'};
        $dbh->{'AutoCommit'} = 0; 
        $dbh->do('BEGIN');
        $self->__query('addSvcLink', binds => [
            my $curId = $self->__nextid('get' => 'services_links'),
            $what2mv,
            $where2place,
            HARD_LINK
        ]);
        $self->__nextid('incr' => 'services_links');
        my $r = $dbh->commit
          or die 'Commit service changes failed: ' . $dbh->errstr;
        $dbh->{'AutoCommit'} = $saveAutoCommit;
        $r
    } else {
    # update existing hard-dependency
        $self->__query('move2OtherParent', subst => {
            new_parent_serviceid 	=> $where2place,
            old_parent_serviceid 	=> $parentids[0],
            child_serviceid		=> $what2mv,
        })
    }
}

# Class method zobjFromSvcName
# Parameters:
# 0:	Service name to extract zoext from
sub zobjFromSvcName {
# this is class-wide method
    shift if blessed($_[0]) and $_[0]->isa(__PACKAGE__);
    # if SCALAR ref was passed - we need to remove zoext from the service name (so parameter will be modified)
    if ( is_scalarref $_[0] ) {
        ${ $_[0] } =~ s%${rxZOSfx}%%;
        return wantarray ? ( $2, $3 ) : $1;
    } else {
        return ( $_[0] =~ $rxZOSfx )[ wantarray ? ( 1, 2 ) : (0) ];
    }
}

sub zobj_get {
  my ( $itsvc, $zoltr, $zoid) = @_;
  ($zoltr, $zoid) = $zoltr =~ /^(.)(.+)$/ if length($zoltr) > 1;
  $itsvc->{'zo'}{$zoltr}{'zobj.get'}->($zoid)
}

sub zobj_exists {
  my ( $itsvc, $zoltr, $zoid) = @_;
  ($zoltr, $zoid) = $zoltr =~ /^(.)(.+)$/ if length($zoltr) > 1;
  $itsvc->{'zo'}{$zoltr}{'exists?'}->($zoid)
}

sub add_zo_attrs {
    my ( $self, $svc, $flResolveZOName ) = @_;
    ( is_plain_hashref($svc) and exists( $svc->{'name'} ) and exists( $svc->{'serviceid'} ) ) or die 'Invalid service object passed to me: insufficient attributes';
    my ( $zoltr, $oid ) =
      $svc->{'triggerid'}
          ? ( 't', $svc->{'triggerid'} )
          : do {
                return $svc unless 
                    $svc->{'name'} =~ s%${rxZOSfx}%%;
                ( $2, $3 );
             };
    my $hndlZO = $self->{'zo'}{$zoltr};
    @{$svc}{ 'ztype', 'zobjid' } = ( $ltr2zobj{$zoltr}{'otype'}, $oid );
    my $zobj = $hndlZO->{'zobj.get'}->($oid) or return $svc;
    $svc->{ $ltr2zobj{$zoltr}{'id_attr'} } = $oid;
    $svc->{'zobj'} = $zobj if $flResolveZOName;
    return $svc;
}

sub get_valid_sid {
    my ( $self, $name_or_sid ) = @_;
    $name_or_sid =~ m/[^\d]/
        ? $self->get_sid_by_name( $name_or_sid )
        : do {
            $self->__query('isSvcExists?', binds => [ $name_or_sid ]) or return; 
            $name_or_sid 
        };
}

sub rename {
    my ( $self, $from, $to ) = @_;
    my $sidFrom = $self->get_valid_sid( $from ) or return {'error' => 'No such IT Service'};
    defined( my $svcName = eval { $self->get_name_by_sid( $sidFrom ) } )
        or return {'error' => $@ ? "while trying to get IT Service name for sid=${sidFrom}: $@" : 'IT Service possibly was removed while trying to rename it'};
    if ( my $zoSfx = zobjFromSvcName($svcName) and !zobjFromSvcName($to) ) {
        $to .= ' ' . $zoSfx;
    }
    $self->__query('renSvcByID', binds => [ $to, $sidFrom ]);
}

sub create_links {
    my ($self, @svcids) = @_;
    return  unless @svcids >= 2;
    my $where2link = pop @svcids;
    my %par4svc = map @{$_}, @{$self->__query('getHardParents', subst => {child_serviceid => [@svcids]})};
    for my $what2link (@svcids) {
      next if $par4svc{$what2link} == $where2link;
      $self->link_it_here($what2link, $where2link, $par4svc{$what2link} ? SOFT_LINK : HARD_LINK);
    }
}

sub unlink {
    my ( $self, $where, $what ) = @_;
    my ($r) = ( eval { $self->__query('unlinkSvc', binds => [$where, $what]) } );
    return {error => $@} unless defined $r;
    return {error => 'Cant break hard-link dependency'} if $r == 0;
    return {unlinked => {parent => $where, child => $what}};
}

sub set_algo {
    state $algo_world2num = +{
      'worst' 	=> SLA_ALGO_ONE_FOR_PROBLEM,
      'avg'	=> SLA_ALGO_ALL_FOR_PROBLEM
    };
    my ( $self, $serviceid, $newalgo ) = @_;
    $self->__query(
      'algochgSvc',
      binds => [__is_pos_integer( $newalgo ) ? $newalgo : $algo_world2num->{lc $newalgo}, $serviceid]
    );
}

sub associate {
    my ( $self, $serviceid, $zobjid ) = @_;
    return { 'error' => 'No such Zabbix object: ' . $zobjid }
      unless $self->{'zo'}{substr $zobjid, 0, 1}{'exists?'}->(substr $zobjid, 1);
    return { 'error' => 'No such IT Service: ' . $serviceid }
      unless $self->{'zo'}{'s'}{'exists?'}->($serviceid);      
    my $svcName = $self->{'zo'}{'s'}{'name.get'}->($serviceid) 
      or return { 'error' => 'No such IT Service: ' . $serviceid };
    my $ltrs    = join( '' => keys %ltr2zobj );
    $svcName =~ s%\s*\([${ltrs}]\d{1,10}\)$%%;
    $svcName .= ' (' . $zobjid . ')';
    $self->{'zo'}{'s'}{'name.update'}->( $serviceid, $svcName );
}

sub deassoc {
    my ($self, $serviceid) = @_;
    my $svcName = $self->{'zo'}{'s'}{'name.get'}->($serviceid) 
        or return { 'error' => 'No such IT Service: ' . $serviceid };
    return {
        'result' => (
              $svcName =~ s%${rxZOSfx}%%
            ? $self->{'zo'}{'s'}{'name.update'}->( $serviceid, $svcName )
            : -1
        )
    };
}

sub getServiceIDsByNames {
    return undef unless @_;
    map {
        if (/[^\d]/) {
            ( my $st = $sql_{'getSvcByName'}{'st'} )->execute($_);
            my @r = @{ $st->fetchall_arrayref( {} ) };
            return undef if @r > 1 or !@r;
            $r[0]{'serviceid'} + 0;
        } else {
            $_ + 0;
        }
    } @_;
}

sub chkZObjExists {
    my $zobjid = shift;
    my $ltrs   = join( '' => keys %ltr2zobj );
    return ()
      unless my ( $objType, $objID ) = $zobjid =~ m/^([${ltrs}])(\d{1,10})$/;
    return $ltr2zobj{$objType}{'check'}{'exists'}->($objID)
      ? ( $objType, $objID )
      : ();
}

sub getITSvcChildrenByName {
    my ( $parSvcID, $svcName ) = @_;
    my $st =
      $sql_{( $parSvcID ? 'getSvcByNameAndParent' : 'getSvcByNameUnderRoot' )
          . ( $svcName =~ $rxZOSfx ? '' : 'RX' ) }{'st'};
    $st->execute( $svcName, $parSvcID ? ($parSvcID) : () );
    return $st->fetchall_arrayref( {} );
}

sub chkITServiceBy_ID_Name_Parent {
    my ( $serviceid, $name, $parentid ) = @_;

    # say Dumper [@sql_{map 'chkSvcIDName'.$_, 'Parent', 'UnderRoot'}];
    my $st =
      $sql_{ 'chkSvcIDName' . ( $parentid ? 'Parent' : 'UnderRoot' ) }{'st'};
    $st->execute( $serviceid, $name, $parentid ? ($parentid) : () );
    return $st->fetchall_arrayref()->[0][0];
}

# Generate Services Tree Branch
sub gen_svc_tree_branch {
    my ( $self, $svc, $parSvcId, $svcName ) = @_;

    is_plain_hashref($svc)
      or confess 'Service must be presented as a hash reference';

    $svcName ||= $svc->{'name'};
    my $svcParents;
    $svc->{'genstat'} =
      _E('Cant create or link unnamed service without valid serviceid'), return
      unless
      __is_pos_integer( $svc->{'serviceid'} )
        ? $svcParents = $self->get_svc_parents( $svc->{'serviceid'} )
        : ( defined($svcName) and length($svcName) );
        
    $svc->{'genstat'} =
      _E( 'Cant link service to an unknown/invalid/unexisting parentid '
          . $svc->{'parentid'} ), return
      if defined( $svc->{'parentid'} ) && ! $self->exists( $svc->{'parentid'} );

    $parSvcId //= $svc->{'parentid'} //=
      ( $svcParents ? $svcParents->[0][PARENTID] : DFLT_ROOT_SERVICEID );
    $svcParents = +{ map @{$_}, @{$svcParents} };
# If serviceid is defined - check, whether this serviceid with the specified name is a child of appropr parent.
# 	If not - check whether this serviceid already exists and if it is - link this serviceid to the specified parent
    if (
      defined $svc->{'serviceid'} and defined $svc->{'parentid'}
        and ! exists( $svcParents->{ $svc->{'parentid'} } ) 
    ) {
        $self->create_links( $svc->{'serviceid'} => $parSvcId )
          and $svc->{'genstat'} = { 'ok' => 'linked' };
    }

    my $svcNodes =
        defined( $svc->{'nodes'} )
      ? is_plain_hashref( $svc->{'nodes'} )
          ? +{
              map {
                  my @kv = each %{$svc->{'nodes'}};
                  scalar( $kv[1]{'name'} || $kv[0] ) => $kv[1]
              } 1 .. keys %{$svc->{'nodes'}}
          }
          : is_plain_arrayref( $svc->{'nodes'} )
        ? +{ map { $_->{'name'} => $_ } @{ $svc->{'nodes'} } }
        : +{}
      : +{};
    $svc->{'genstat'} = { 'ok' => 'exists' }, return 1
      if defined $svc->{'serviceid'} and ! %{$svcNodes};
    unless ( defined $svc->{'serviceid'} ) {
        my $flSameSvcDel;
        my @sameSvcs = $self->get_sid_by_name( $svcName, $parSvcId );
        if ( exists $svc->{'recreate'} and @sameSvcs ) {
            if ( $svc->{'recreate'} ) {
                if ( $#sameSvcs ) {
                  $svc->{'genstat'} = _E('Cant recreate service: there are more than one services with the same name under same parent (so we dont know what service to delete)'),
                  return
                }
                $self->delete( $sameSvcs[0] );
                $flSameSvcDel = 1;
            } else {
                $svc->{'serviceid'} = $sameSvcs[0];
                $svc->{'genstat'}   = { 'ok' => 'exists' };
                goto RECURSE_NODES;
            }
        }
        my %svcSettings = (
            'algorithm' => SLA_ALGO_ALL_FOR_PROBLEM,
            'showsla'   => SHOW_SLA_CALC,
            'goodsla'   => DFLT_GOOD_SLA,
            'sortorder' => 0,
            'triggerid' => undef,
        );

# Hint: 'triggerid' is absent in %svcSettings, so we need to explicitly put it in @k
        my @k = grep exists $svcSettings{$_}, keys %{$svc};
        @svcSettings{@k} = @{$svc}{@k} if @k;
        my $res = $self->create(
            {
                %svcSettings,
                'name'     => $svcName,
                'parentid' => $parSvcId,
            }
        );
        $svc->{'genstat'} =
          _E(  
            is_hashref($res) && defined $res->{'error'}
              ? $res->{'error'}
              : 'UNKNOWN'
          ), return
             if ! is_hashref($res)
                 or   defined( $res->{'error'} )
                 or ! defined( $res->{'serviceids'} );
        $svc->{'serviceid'} = $res->{'serviceids'}[0];
        $svc->{'genstat'} = { 'ok' => $flSameSvcDel ? 'recreated' : 'created' };
    }

  RECURSE_NODES:
    return 1 unless %{$svcNodes};
    my $parId = $svc->{'serviceid'};

    # say Dumper $svcNodes;
    while ( my ( $svcName, $svc ) = each %{$svcNodes} ) {
        $self->gen_svc_tree_branch( $svc, $parId, $svcName );
    }
    return $svc;
}

sub getITServiceAPI {
    my ( $svcParent, $serviceGetPars ) = @_;
    my $childSvcs = zbx(
        'service.get',
        {
            %{$serviceGetPars},
            'serviceids'         => $svcParent->{'serviceid'},
            'selectDependencies' => ['serviceid']
        }
    );
    return undef unless ref($childSvcs) eq 'ARRAY' and @{$childSvcs};
    for my $refDep ( map { map \$_, @{ $_->{'dependencies'} } }
        grep { !$_->{'triggerid'} and @{ $_->{'dependencies'} } }
        @{$childSvcs} ) {
        $$refDep = getITServiceAPI( $$refDep, $serviceGetPars );
        delete $$refDep->{'triggerid'} unless $$refDep->{'triggerid'};
    }
    return scalar( $#{$childSvcs} ? $childSvcs : $childSvcs->[0] );
}

sub get_name_by_sid {
    my ($self, $svcid) = @_;
    my $name =
    $svcid == DFLT_ROOT_SERVICEID
        ? DFLT_ROOT_NAME
        : do { 
            defined( 
                my $name = eval { $self->__query('getNameById', binds => [$svcid]) }
            ) or die "Cant determine name for IT Service #${svcid}";
            $name
        };
    utf8::decode( $name ) unless utf8::is_utf8( $name );
    $name
}

# Arguments:
# 0: full-path-to-service as:
#		 [path, to, service] 
#			or "path/to/service"
#			or maybe "/path/to/service"
# 1: parent service id which is a "root" for path-to-service
sub get_sid_by_name {
    my $self = shift;
    my @names =
      is_plain_arrayref($_[0])
          ? @{ $_[0] }
          : do { my @snh = split /\// => $_[0]; shift @snh if $snh[0] eq ''; @snh };
    my $parSvcID = $_[1];

    my @svcids = map $_->[0], @{$self->__query(
        'getSvcByNameAndParent',
        binds => [ scalar( shift @names ) =~ s%([()])%\\$1%gr ],
        subst => {parentid => $parSvcID || undef},
        method_args => [],
    )} or return;
    
    @names 
# here we have to create copy of @names to avoid side-effect of shift'ing    
      ? (map { my @names_copy = @names; $self->get_sid_by_name( \@names_copy, $_ ) } @svcids)
      : @svcids
}

sub get_associated_svc {
    my ($self, $zobjid) = @_;
    $self->__query('getSvcByZOExt', binds => [$zobjid], as_hash_refs => true);
}

sub get_svc_tree_branch {
    my ( $self, $svc, $csh ) = @_;
    unless ( ref $svc ) {
        ( __is_pos_integer($svc) and $svc = $self->zobj_get( 's', $svc ) )
          or die 'wrong serviceid';
        $csh = {};
    }
#    print Dumper $svc;
    my $serviceid = $svc->{'serviceid'};
    return if $csh->{$serviceid}{'rflag'};
    return $csh->{$serviceid}{'obj'} if defined $csh->{$serviceid}{'obj'};
    my ( $zoType, $zoID ) = zobjFromSvcName( $svc->{'name'} );
    $csh->{$serviceid}{'rflag'} = 1;    # prevent-recursion-flag
    utf8::decode( $svc->{'name'} );
    if ( defined(my $triggerid = $svc->{'triggerid'}) ) {
        $svc->{'ztype'}  = 'trigger';
        $svc->{'zobjid'} = $triggerid;
        my $trg = $self->zobj_get('t' => $triggerid);
        $svc->{'lostfunk'} = ( $trg->{'priority'} - 1 ) / 4
          if $trg->{'value'}
            and ! $trg->{'status'}
            and   $trg->{'priority'} > 1;
    } else {
        if ( my ( $zoType, $zoID ) = zobjFromSvcName( \$svc->{'name'} ) ) {
            if ( $zoType eq 't' ) {
                $svc->{'invalid'} = 1;
                return $svc;
            }
            if ( defined( my $zoDscrByType = $ltr2zobj{$zoType} ) ) {
                $svc->{'ztype'}                      = $zoDscrByType->{'otype'};
                $svc->{'zobjid'}                     = $zoID;
                $svc->{ $zoDscrByType->{'id_attr'} } = $zoID;
            }
        }
        delete $svc->{'triggerid'};
        if ( $zoType eq 'h' ) {
            unless ( my $hostStatus = $self->__query('checkHostEnabled', binds => [$zoID]) )
            {
                $svc->{'unfinished'} = 1;
                if ( defined $hostStatus ) {
                    $svc->{'disabled'} = 1;
                } else {
                    delete $svc->{'hostid'};
                }
            }
        }
        unless ( exists $svc->{'disabled'} ) {          
            if ( my @deps =
                grep { !exists $_->{'invalid'} }
                  map { return unless my $t = $self->get_svc_tree_branch($_, $csh); $t }
                    @{ $self->__query('getSvcDeps', binds => [$serviceid], as_hash_refs => 1) } )
            { # if @deps ...

                if ( my @ixTermDeps =
                    grep { !exists $deps[$_]{'unfinished'} } 0 .. $#deps ) {
                    my $lostFunK = 0;
                    my $childLFKWeight =
                      $svc->{'algorithm'} == SLA_ALGO_ALL_FOR_PROBLEM
                      ? ( 1 / @ixTermDeps )
                      : 1;
                    $lostFunK += $_ * $childLFKWeight
                      for grep $_, map $deps[$_]{'lostfunk'}, @ixTermDeps;
                    $svc->{'lostfunk'} = $lostFunK > 1 ? 1 : $lostFunK
                      if $lostFunK;
                } else {
                    $svc->{'unfinished'} = 1;
                }
                $svc->{'dependencies'} = \@deps;
            } else {
                $svc->{'unfinished'} = 1;
            }
        }
    }
    $csh->{$serviceid}{'rflag'} = 0;
    $csh->{$serviceid}{'obj'}   = $svc;
}

sub get_all_deps {
    my ($itsvc, $svc) = @_;
    sub svcDepsClean {
        my $svc = shift;
        return +{
          map { $_ => $svc->{$_} }
            grep { $_ ne 'dependencies' } keys %{$svc}
        }
    }

    sub getDepsRecursive {
        my $svc = shift;
        return {} unless ref $svc;
        return svcDepsClean($svc)
          unless defined( $svc->{'dependencies'} )
          and @{ $svc->{'dependencies'} };
        return (
            svcDepsClean($svc),
            map getDepsRecursive($_),
            @{ $svc->{'dependencies'} }
        );
    }
    getDepsRecursive( $itsvc->get_svc_tree_branch($svc) );
}

sub get_all_by_type {
  my ($self, $zoltr) = @_;
  $self->__query('getAllSvcAssocWithZType', binds => [$zoltr]);
}

sub get_deps_by_type {
    my ( $itsvc, $rootSvcID, $zoltr ) = @_;
    return { 'error' => 'Wrong parameters passed: serviceid must be positive integer, zabbix object type identifier - one letter' }
      unless $ltr2zobj{$zoltr} and __is_pos_integer( $rootSvcID );
    return { 'error' => 'Base ITService with the specified ID not found' }    
      unless !$rootSvcID or $itsvc->exists($rootSvcID);
    my ( $ztype, $idattr ) = @{ $ltr2zobj{$zoltr} }{qw(otype id_attr)};
#    $rootSvcID or zbx_api_url() or  
#      return { 'error' =>
#			'You must properly initialize Zabbix API before passing base serviceid=0 to getITServiceDepsByType'
#      };
    my @svcs =
      $rootSvcID
      ? grep { defined( $_->{'ztype'} ) and $_->{'ztype'} eq $ztype }
          $itsvc->get_all_deps($rootSvcID)
      : map {
          $_->{'name'} =~ s%^(.+)\s+\(${zoltr}(\d+)\)$%$1%
            ? do {
                $_->{$idattr} = $2;
                ($_)
              }
            : ()
      } @{$itsvc->get_all_by_type($zoltr)};
#      @{
#        zbx(
#            'service.get',
#            {
#                'search' => { 'name' => "*(${zoltr}*)" },
#                'output' => ['name']
#            }
#        )
#      };
}

sub __is_pos_integer {
    return unless @_ and defined $_[0];
    my $fl = looks_like_number( $_[0] ) or return;
    ( $fl == DEF_INTEGER_AS_STRING and index( $_[0], '-' ) < 0 )
      or $fl == DEF_POSITIVE_INTEGER;
}

sub __filter_hash_ref {
    my ($hr, $valid_keys) = @_;
    delete @{$hr}{grep !exists($valid_keys->{$_}), keys %{$hr}};
    %{$hr} or die 'no valid keys in passed hashref after filtering';
    $hr
}

sub __sprintf {
    sprintf $_[0][0], @{$_[0]}[1..$#{$_[0]}]
}


sub __turn2list {
  map is_arrayref($_) ? @{$_} : ($_), @_;
}

1;

