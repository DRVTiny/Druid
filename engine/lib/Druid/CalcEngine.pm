#!/usr/bin/perl
package Druid::CalcEngine;
use 5.16.1;
use strict;
use warnings;
use utf8;
use experimental qw(smartmatch);
BEGIN { binmode $_, ':utf8' for *STDOUT,*STDERR; }

use Fcntl;
use Scalar::Util 	qw(blessed refaddr);
use Ref::Util 		qw(is_plain_hashref is_plain_arrayref is_hashref is_arrayref);
use List::Util 		qw(max min sum all any notall uniq);
use POSIX 		qw(strftime);
use Data::Dumper 	qw(Dumper);
use Carp 		qw(croak confess);
use JSON::XS 		qw(encode_json decode_json);
use lib '/opt/Perl5/libs';
use Tag::DeCoder;
use POSIX::RT::Semaphore;
use Log::Log4perl;
use Log::Log4perl::KISS;
use Try::Tiny;
use RedC;
use DBI;
use ZAPI;
use DBR;

use FindBin;
BEGIN {
    $ENV{'DRUID_MODE'} and $ENV{'DRUID_MODE'} eq 'development' and unshift @INC, "$FindBin::Bin/../lib";
}
use Druid::ZTypes qw(%_sql %zobjTypes);

use constant {
    ROOT_SERVICE_ID		=>  0,
    
    LEAF_SERVICE_NESTD		=>  0,
    
    RECONNECT_UP_TO		=>  10,		# Redis reconnect up to X seconds
    RECONNECT_EVERY		=>  100_000,	# Redis try to reconnect every Y microseconds
    SVC_ALGO_ONE_FOR_PROBLEM	=>  1,
    SVC_ALGO_ALL_FOR_PROBLEM 	=>  2,
    
    TRIG_PRIO_INFO		=>  1,
    TRIG_PRIO_WARN      	=>  2,
    TRIG_PRIO_AVG       	=>  3,
    TRIG_PRIO_ERROR     	=>  4,
    TRIG_PRIO_DISASTER		=>  5,
    
    LFK_OK			=>  0,
    LFK_DISASTER		=>  1,
    LFK_UNKNOWN			=> -8,
    LFK_LOOP_PROTECT		=> -10,
    LFK_DISABLED		=> -12,
    LFK_MAINTENANCE		=> -13,
    LFK_BEHIND_THE_DEP		=> -16,
    LFK_NOT_CALC                => -18,
    
    PREV_LFK			=>  0,
    CURR_LFK			=>  1,
    
    LFK_DELTA_TOLERANCE		=>  1e-3,
    
    BM_DONT_UPDATE_CUR_TRG 	=>  8,
    BM_RE_INIT_CUR_TRG		=>  4,
    BM_DEL_SEM_IF_EXIST		=>  2,
    
    GROUP_CONCAT_MAX_LEN	=>  24576, # SELECT GROUP_CONCAT() size limitation (bytes)
    
    YES				=>  1,
    NO				=>  0,
    TRUE			=>  1,
    FALSE			=>  undef,
    DONE			=>  1,
    
    DFLT_SERIALIZER		=> 'CB', # Serialize to CBOR by default
    
    CACHE3_REDIS_DB_N		=>  4,
    SQL_DBMS_NAME		=>  17,
};

use constant RN_VAL_TRIG_PRIO => 1 / (TRIG_PRIO_DISASTER - TRIG_PRIO_INFO);

use subs qw/__fq_func_name __get_trigger_lostfunk __hp_timer __json __va_array_ref/;

my %SVC_ROOT_NODE = (
    'serviceid' => 0,
    'name' 	=> '<ROOT>',
    'algorithm' => SVC_ALGO_ALL_FOR_PROBLEM
);

my $ltrs = join('' => keys %{$zobjTypes{'by_letter'}} );
my $rxZOExt = qr/^(?:.+\s+)?\(([${ltrs}])(\d+)\)$/;
my $rxZOExt4s = qr/\s*\(([${ltrs}])(\d+)\)$/;
my $json = JSON::XS->new;


sub new {
    my ($class, %pars) = @_;
    my $zapi = ZAPI->new(undef, 'DBIx::SQLEngine');
    # $dbhR will be Instance of DBIx::SQLEngine::* (driver for target database) by default
    my $dbhR = $zapi->dbh;
    ref($dbhR) =~ /^DBI(x::SQLEngine|::db)/ or die sprintf 'Cant work with dbh provided by ZAPI instanciated not from DBIx::SQLEngine or DBI::db classes, but received inastance of <<%s>> class instead. Please check DB_PERL_PKG option in your ZAPI configuration', ref($dbhR);
#    print Dumper \%zobjTypes; exit;
    my $logger = sub {
        my $L = shift;
        ($L and ref($L) and blessed($L) and !(grep !$L->can($_), qw/debug info warn error fatal/))
            ? $L
            : do {
                Log::Log4perl::initialized() or Log::Log4perl::init(\<<'EOLOGCONF');
log4perl.rootLogger                      =      DEBUG, Screen

log4perl.appender.Screen                 =      Log::Log4perl::Appender::Screen
log4perl.appender.Screen.stderr          =      1
log4perl.appender.Screen.layout             =      Log::Log4perl::Layout::PatternLayout
log4perl.appender.Screen.layout.ConversionPattern = %d{HH:mm:ss} | %d{dd.MM.yyyy} | %P | %p | %m%n
EOLOGCONF
                Log::Log4perl->get_logger();
              }
    }->($pars{'logger'});
    my $encoder = uc($pars{'encoder'} // DFLT_SERIALIZER);
    my %props; %props = (
        'dbhSource'	=>	{ 'value' => $dbhR, 'protected' => TRUE },
        'zapi'		=> 	{ 'value' => $zapi, 'read_only' => TRUE, 'protected' => TRUE },
        'redC'		=>	{
        # Name must be unique to be possible to exactly identify this client amongst others in the Redis client list
            'value' 	=> RedC->new( 'name' => join('::' => __PACKAGE__, refaddr(\%props)), 'encoder' => $encoder ),
            'read_only' => TRUE,
        },
        'cache3redisDbN'	=>	{ 'value' => CACHE3_REDIS_DB_N, 		'protected' => TRUE },
        'zbxMajorVersion'	=>	{ 'value' => substr($zapi->zversion, 0, 1), 	'protected' => TRUE },
        'servicesOfInterest'	=>	{ 'value' => {}, },
        'underRootServices'  	=>      { 'value' => {}, 'read_only' => TRUE, },
        'encoder'		=>	{ 'value' => $encoder, 'read_only' => TRUE, },
        'st'			=>	{ 'value' => undef, 'read_only' => TRUE, },
        'semlocks'		=>	{ 'value' => {}, 'protected' => TRUE, },
        'logger'		=>	
        { 
            'value' => undef, 
            'setter' => sub {
                my $L = shift;
                return unless $L and ref($L) and blessed($L) and !(grep !$L->can($_), qw/debug info warn error fatal/);
                $props{'logger'}{'value'} = $L
            }
        },
    );
    my $slf; $slf = bless sub {
        @_ or croak 'Arguments expected';
        my ($arg0, $r_arg0) = ($_[0], ref($_[0]));
        my $callerPkg = (caller)[0];
        my $flCallerIsTrusted = ($callerPkg eq __PACKAGE__) || (index($callerPkg, __PACKAGE__ . '::') == 0);
        do { print Dumper \%props; exit; } if $arg0 eq 'CRASHDUMP';
        croak 'First argument must be: SCALAR, HASHREF or ARRAYREF' if $r_arg0 and !($r_arg0 eq 'HASH' or $r_arg0 eq 'ARRAY');
        if ( $r_arg0 eq 'HASH' ) {
            my $parHsh = $r_arg0 eq 'HASH'? $arg0 : {@_};
            while ( my ($par, $val) = each %{$parHsh} ) {
                croak 'Cant set undefined or read-only or protected property: '.$par
                    unless $par and exists($props{$par}) and !( ($props{$par}{'protected'} and !$flCallerIsTrusted) or $props{$par}{'read_only'} );
                $props{$par}{'setter'}
                    ? $props{$par}{'setter'}->($val)
                    : ($props{$par}{'value'} = $val);
            }
            return DONE
        } elsif ($r_arg0 eq 'ARRAY' or !${r_arg0}) {
            my @out=map {
                croak 'Expected: property name. References are not acceptable here' if ref $_;
                croak 'Property name cant be empty string' unless $_;
                croak "Unknown property $_" unless exists $props{$_};
                croak "Property $_ is protected" if $props{$_}{'protected'} and (caller)[0] ne __PACKAGE__;
                $props{$_}{'value'}
            } $r_arg0 eq 'ARRAY'? @{$arg0} : @_;
            return wantarray ? @out : (@out > 1 ? \@out : $out[0]);
        } else {
            logdie_('Reference of type '.${r_arg0}.' can not be accepted as a first parameter. Possible arguments: {key0=>value0,key1=>value1,...} or key0,key1,..,keyN or [key0,key1,..,keyN]');
        }
    }, $class;    
#    print Dumper \%props; exit;
    
    $props{'st'}{'value'} = \my %sqlSt;
    # prepare %_sql statements and place it into to %sqlSt==$props{'st'}
    while (my ($sqlOpName, $sqlOpDscr) = each %_sql) {
        # replace static parts of the query (constants)
        $sqlOpDscr->{'rq'} =~ s/%\{([^}]+)\}/eval($1)/ge;
        
        # dont create statement handle early if query contains any dynamic parts such as "%s" for sprintf() or ${CODE} for eval "CODE"
        next if (my $query = $sqlOpDscr->{'rq'}) =~ m/(?:(?:^|(?<!%))%s|\$\{)/;
        {
            no strict 'refs';
            $query =~ s%\{\{([^}(]+)\((.*?)\)\}\}%$1->($slf,split(/,\s*/,$2))%gex;
        }
        $sqlSt{ $sqlOpName } = $dbhR->prepare($query);
    }
    
    my $stGetUnderRootSvcs = $sqlSt{'svcGetIDsOfRootDeps'};
    $stGetUnderRootSvcs->execute();
    $props{'underRootServices'}{'value'} = +{ 
        map { $_->[0] => 1 } @{$stGetUnderRootSvcs->fetchall_arrayref([])}
    };
    
    $props{'servicesOfInterest'}{'value'} =
    (
        $pars{'root_services'} &&
        is_plain_arrayref($pars{'root_services'}) && 
        ! ( grep { ! length($_) or /[^\d]/ } @{$pars{'root_services'}} and croak 'root_services list must contain only numeric service ids' )
    ) 
        ? +{map { $_ => 1 } @{$pars{'root_services'}}}
        : $props{'underRootServices'}{'value'};
        

    $slf->({'logger' => $logger});
    debug { "*************** %s is ready to use ****************\nZabbix info:\n\tdatabase type: %s\n\tmajor version: %d" } __PACKAGE__, uc($zapi->dbtype), $slf->('zbxMajorVersion');
    for my $zotypeConf ( values %{$zobjTypes{'by_name'}} ) {
        my $zoltr = $zotypeConf->{'letter'};
        $zotypeConf->{'table'} = ${$zapi->fixed_table_name($zotypeConf->{'table'})};
        $props{'st'}{'value'}{'getzobj'}{$zoltr} = $dbhR->prepare(
            sprintf('select %s from %s where %s=?', join(',' => @{$zotypeConf->{'xattrs'}}), @{$zotypeConf}{'table','id'})
        );
    }
    return $slf;
}

sub get_redc_for {
    my $slf = $_[0];
    my $ztype = lc $_[1];
    
    my $zot = $zobjTypes{
        'by_' . ( length($ztype) > 1 ? do { substr($ztype, -1, 1) = '' if substr($ztype, -1, 1) eq 's'; 'name' } : 'letter' )
    }{$ztype} or logdie_ 'Dont know anything about zobj type <<%s>>', $ztype;
    defined( my $redis_db_n = $zot->{'redis_db'} ) or logdie_ 'There is no redis_db_n definition for zobj type <<%s>>', $ztype;
    $slf->('redC')->select( $redis_db_n )
}

sub get_zobj_from_src {
    my $slf = shift;
    my ($zoltr, $zoid) = 
    ( length($_[0]) > 1 and !$#_ )
        ? ( substr($_[0], 0, 1), substr($_[0], 1) )
        : ( substr($_[0], 0, 1), $_[1] );
    return if !$zoid or $zoid =~ /[^\d]/;
    return unless my $st = $slf->('st')->{'getzobj'}{$zoltr};
    $st->execute($zoid);
    eval { $st->fetchall_arrayref({})->[0] }
}

sub get_many_zobjs_from_src {
   my ($slf, $zoltr) = splice @_, 0, 2;
   (my $zotypeConf = $zobjTypes{'by_letter'}{$zoltr}
       and
   my %zoids = map {$_ => 1} grep {length and !/[^\d]/} is_plain_arrayref($_[0]) ? &__flatn : @_)
       or return;
    my $dbh = $slf->('dbhSource');
    my $st = $dbh->prepare(
        ($zotypeConf->{'sql'} and defined( $_sql{$zotypeConf->{'sql'}} ))
            ? sprintf( $_sql{$zotypeConf->{'sql'}}{'rq'}, join(',' => keys %zoids) )
            : sprintf('select %s from %s where %s in (%s)', 
                join( ',' => @{$zotypeConf->{'xattrs'}} ),
                $zotypeConf->{'table'},
                $zotypeConf->{'id'},
                join( ',' => keys %zoids )
              )
    );
    $st->execute();
    $st->fetchall_hashref( $zotypeConf->{'id'} );
}

sub __get_all_svc_deps {
    (my $sth = $_[0]->__get_sth('svcGetPar2Child'))->execute();
    my @par2deps;
    push @{$par2deps[$_->[0]]}, $_->[1] for @{$sth->fetchall_arrayref()};
    return \@par2deps;
}

sub reloadCache2 {
    state $assocWithServiceIdAttr={'h'=>1, 'g'=>1};
    my ($slf, %opt) = @_;
    
    my $dbhR = $slf->('dbhSource')
        or logdie_ 'Cant read from source db: it is not initialized yet';
#    $dbhR->ping or $dbhR->clone;
    
    my $svc_par2deps = $slf->__get_all_svc_deps();
    
    (my $stGetAllSvc = $slf->__get_sth('getAllSvcsWithParents'))->execute;
    
    my (%svcs, %assocs, @delSvcTrgs);
    
    for my $svc ( @{$stGetAllSvc->fetchall_arrayref({})} ) {
         
        my $svcid = $svc->{'serviceid'};
        next if exists $svcs{$svcid};
        if ( defined(my $deps = $svc_par2deps->[$svcid]) ) {
            $svc->{'dependencies'} = +{ map {$_ => 1} @{$deps} }
        }
        for (qw(triggerid parents)) {
            unless ( $svc->{$_} ) {
                delete $svc->{$_}
            } elsif ($_ eq 'parents') {
                $svc->{'parents'} = [split /,/ => $svc->{$_}]
            }
        }
        $svcs{$svcid} = $svc;
        my $triggerid = $svc->{'triggerid'};
        # Determine, whether service name extension present
        # ($flZOExtHere will be == count of succesfull replacements, i.e. 1 if extension present and undef if it is not so)
        my $flZOExtHere = $svc->{'name'} =~ s%${rxZOExt4s}%%;
        # Extract zoltr (object type) and zoid (object id) for zabbix object associated with processed service
        next unless my ($zoltr, $zoid) = $triggerid ? ('t',$triggerid) : ( $flZOExtHere ? ($1, $2) : () );
        if ($zoltr eq 't' and not (defined $triggerid)) {
            push @delSvcTrgs, $svcid;
            next
        }
        # If serviceName stays empty after removing of its associative extension AND if this service was associated with some trigger 
        #  - recover serviceName as containing only extension "(t<triggerid>)"
        $svc->{'name'} = "(t${triggerid})" if ($flZOExtHere and !$svc->{'name'} and defined($triggerid));
       
        $svc->{'zloid'} = $zoltr . $zoid;
        $assocs{$zoltr}{$zoid} = $svcid;
    }
    __purge_svcs(\%svcs, @delSvcTrgs) if @delSvcTrgs;
    my @rootDeps = grep defined $_, map $svcs{$_}, keys %{$slf->('underRootServices')};
    $svcs{0} = {%SVC_ROOT_NODE, 'dependencies'=>{map { $_->{'under_root'}=1; $_->{'serviceid'}=>1 } @rootDeps}};
    my %zo=(
        map {
            my ($zoltr, $zoids)=($_, $assocs{$_});
            my $zobjs = $slf->get_many_zobjs_from_src($zoltr, keys %{$zoids});
                $zoltr => 
                    $assocWithServiceIdAttr->{$zoltr}
                        ? +{map { my ($oid, $me)=each $zobjs; $me->{'serviceid'}=$zoids->{$oid}; $oid => $me } 1..keys %{$zobjs}}
                        : $zobjs
        } keys %assocs
    );
    
    @delSvcTrgs = ();
    for ( map { 
            exists($_->{'zloid'}) 
                ? do { my $zloid = $_->{'zloid'}; defined $zloid and !defined($zo{substr $zloid, 0, 1}{substr $zloid, 1}) ? ([substr($zloid,0,1), $_]) : () } 
                : ()
          } values %svcs 
    ) {
        my ($zoltr, $svc) = @{$_};
        if ($zoltr eq 't') {
            push @delSvcTrgs, $svc->{'serviceid'}
        } else { 
            delete $svc->{'zloid'}
        }
    }
    
    __purge_svcs(\%svcs, @delSvcTrgs) if @delSvcTrgs;
    
    for ( values %svcs ) {
       $_->{'lostfunk'} = LFK_NOT_CALC;
       $_->{'dependencies'} = [keys %{$_->{'dependencies'}}] if exists $_->{'dependencies'};
       if ( $_->{'triggerid'} ) {
          push @{$zo{'t'}{$_->{'triggerid'}}{'svcpath'}}, __trace_svc_path(\%svcs, $_->{'serviceid'})
       }
       $zo{'s'}{$_->{'serviceid'}} = $_;
    }
    __time(undef); # Cache current Unix timestamp
    doCalcLostFunK(ROOT_SERVICE_ID, \%zo, \my %trigLFKs) or logdie_('Cant initially calculate services tree');
    {
        my $ttt = $zo{'t'};
        $ttt->{$_}{'lostfunk'} = $trigLFKs{$_} for keys %trigLFKs;
    }
    if ( my $fhPrintTarget = $opt{'only_print_zo'} ) {
        say { ref $fhPrintTarget eq 'GLOB' ? $fhPrintTarget : *STDERR } $json->encode(\%zo);
        return DONE
    }

    my $res = $slf->writeZObjs2SepDbs(\%zo, \%zobjTypes);
    defined($res->[0]) or logdie_ 'Error while initializing cache-level-2:', $res->[1]{'error'};
    
    return $opt{'mem_safe'} ? DONE : \%zo;
}

sub writeZObjs2SepDbs {
    my ($slf, $zo, $zoTypes) = @_;
    my $redC = $slf->('redC');
    my ($ztype, $redisDbIndex);
    try {
        while ( my ($zoltr, $zobjs) = each %{$zo} ) {
            my $zot = $zoTypes->{'by_letter'}{$zoltr};
            ( is_plain_hashref($zot) and $ztype = $zot->{'type'} )
                or return [undef, {'error' => sprintf 'ZObj type <<%s>> is not supported by %s', $zoltr, __PACKAGE__}];
            debug { 'Processing zobjs type: <<%s>>', $ztype };
            defined( $redisDbIndex = $zot->{'redis_db'} ) 
                or return [undef, {'error' => sprintf 'Cant write ZObjs of type <<%s>> to Redis: database not specified in zobjTypes', $ztype}];
            debug { "select( $redisDbIndex )"};
            $redC->select( $redisDbIndex );
            $redC->multi;
            debug { "flushdb( $redisDbIndex )"};
            $redC->flushdb;
            $redC->write_not_null( $zobjs => sub {
                if ( $_[1] ) {
                    $redC->discard;
                    confess "Redis reports problem while writing ${ztype}-objects to cache: $_[1]"
                }
                debug { '%i objects of type "%s" was written to Redis Db #%d', scalar(keys %{$zobjs}), $ztype, $redisDbIndex };
            });
            debug { "before wait_all_responses" };
            $redC->wait_all_responses;
            $redC->exec;
        }
        $redC->select( $slf->('cache3redisDbN') );
        $redC->set('reload_ts', Time::HiRes::time);
    } catch {
        my $err = $_;
        error_ "Catched error: $err";
        $redC->discard;
        return [
            undef,
            { 'error' => sprintf 'Cache init: error while trying to write objects of type "%s" to Redis Db #%d: %s', $ztype, $redisDbIndex, $err }
        ]
    };
    return [1]
} # <- writeZObjs2SepDbs()

sub actualizeHosts {
    my $slf = $_[0];
    my $redcHostsDb = $slf->get_redc_for('hosts');
    my $st = $slf->__get_sth('getSvc2Host');
    $st->execute();
    my $hostsActual = $st->fetchall_hashref('hostid');
    return unless %{$hostsActual};
    my $hostsCached = $redcHostsDb->read({}, keys %{$hostsActual});
    if ( my %updHosts = map {
        my ($hostid, $cshHost) = each %{$hostsCached};
        if ( defined $cshHost ) {
            my $actHost = $hostsActual->{$hostid};
#        if ( defined(my $actHost = $hostsActual->{$hostid}) ) {
            scalar(grep defined($actHost->{$_} == $cshHost->{$_} ? undef : ($cshHost->{$_} = $actHost->{$_})), qw/maintenance_status status/)
                ? ($hostid => $cshHost)
                : ()
        } else {
            ()
        }
    } 1..keys %{$hostsCached} ) {
    
        $redcHostsDb->write(\%updHosts, sub { 
            logdie_ 'Failed to update hosts status in Redis:', $_[1] if defined($_[1]);
            info_ 'Status of %d hosts was updated', scalar(keys %updHosts)
        });
        
        $redcHostsDb->wait_all_responses;
    } else {
        debug { 'All (act=%d, csh=%d) hosts status is actual', (map scalar(keys %{$_}), $hostsActual, $hostsCached) };        
    }
    1;
}

# Bubble maintFlag over tree under the service associated with the host which maintenance_status was changed.
sub actualizeMaintFlag {
    my ($slf, $hostid, $flInMaintenance) = @_;
    my $host = $slf->get_redc_for('hosts')->read_not_null($hostid)->[0];
    my $redcServices = $slf->get_redc_for('services');
    my %seenNodes;
    sub {
        my ($callNodeId, $ansNodeIds, $callNode) =
            $_[1]
                ? @_[0, 1]
                : do {
                    my $node = $redcServices->read_not_null($_[0])->[0];
                    ($_[0], __va_array_ref( $node->{'parents'} ), $node)
                  };
        return unless my %affectedNodes = (
            $callNode ? ($callNodeId => $callNode) : (),
            $ansNodeIds
            ? do { my $i = 0; map {
                my $ansNode = $_;
                my $ansNodeId = $ansNodeIds->[$i++];
                $seenNodes{$ansNodeId} = TRUE;
                ($flInMaintenance xor $ansNode->{'maintenance_flag'})
                ? do {
                    my @siblings = grep {$_ != $callNodeId} @{$ansNode->{'dependencies'}};
                    ($flInMaintenance
                        ? ($ansNode->{'algorithm'} == SVC_ALGO_ONE_FOR_PROBLEM 
                                or 
                           ! ( @siblings and notall { $_->{'maintenance_flag'} } @{$redcServices->read( @siblings )} )
                          )
                        : ($ansNode->{'algorithm'} == SVC_ALGO_ALL_FOR_PROBLEM
                                or
                            ! (@siblings and any { $_->{'maintenance_flag'} } @{$redcServices->read( @siblings )})
                          )
                    ) ? ($ansNodeId => $ansNode) : ();
                  }
                : ()
              } @{$redcServices->read_not_null( @{$ansNodeIds} )} }
            : ()
        );
        while (my ($affectedNodeId, $affectedNode) = each %affectedNodes) {
            $flInMaintenance
                ? ($affectedNode->{'maintenance_flag'} = TRUE)
                : do { delete $affectedNode->{'maintenance_flag'} if exists $affectedNode->{'maintenance_flag'} };
            if ( my @parids = grep !exists($seenNodes{$_}), @{$affectedNode->{'parents'} || []} ) {
                __SUB__->($affectedNode, \@parids)
            }
        }
        $redcServices->write_not_null( \%affectedNodes );
    }->( $host->{'serviceid'} );
}

sub actualizeTrigValues {
    my ($slf, %pars) = @_; 
    my $flags = $pars{'flags'} // 0;
    my $funcName = __fq_func_name;
    ref($slf) eq __PACKAGE__
        or logdie_( '%s(): you must pass object as a first parameter or call this function as object method', $funcName );
    my $semPath = '/'.$funcName.'_'.($ENV{'LOGNAME'} || $ENV{'USER'} || getpwuid($<));
    ref(my $lock = POSIX::RT::Semaphore->open($semPath, O_CREAT, 0600, 1)) =~ m/^POSIX::RT/
        or logdie_("Cant create <<${semPath}>> lock to update triggers safely: " . $!);
    unless ( $lock->trywait() and $slf->('semlocks')->{$semPath} = $lock) {
        debug { "SEMVAL($semPath)=".$lock->getvalue() };
        if ($flags & BM_DEL_SEM_IF_EXIST) {
            POSIX::RT::Semaphore->unlink($semPath);
            $lock = POSIX::RT::Semaphore->open($semPath, O_CREAT, 0600, 1);
            logdie_ 'Cant create lock after unlinking' unless $lock->trywait();
        } else {
            logdie_('Another instance of trigger actualization function already running');
        }
    }
    
    try {
        my $stGetActualTrigs = $slf->__get_sth('getListedTrigObjs');
        debug { __hp_timer(); 'Retrieving trigger values from Zabbix database (i.e. actual values)...' };
        $stGetActualTrigs->execute();
        $stGetActualTrigs->rows or logdie_( 'No triggers associated with IT-services found in source db' );
        my $curTrigs = $stGetActualTrigs->fetchall_hashref( 'triggerid' );
        debug { '[%f sec.] %d current trigger values retrieved', __hp_timer(), scalar(keys %{$curTrigs}) };
        my $redcTrigsDb = $slf->get_redc_for('triggers');
        debug { __hp_timer(); 'Retrieving trigger values stored in Redis DB#%d (i.e. old, previous values)' } $redcTrigsDb->index;
        
        my $prvTrigs = do {
            my @triggerids = $redcTrigsDb->keys('*');
            my $c = 0; 
            +{ map { $triggerids[$c++] => decodeByTag($_) } $redcTrigsDb->mget( @triggerids ) };
        };
        
        debug { '[%f sec.] %d previous trigger values retrieved. Calculating diffTrigs based on prv_trg and cur_trg comparison', __hp_timer(), scalar(keys %{$prvTrigs}) };
        
        if (
            my @diffTrigs = map { 
                my ($triggerid, $ptrg) = each %{$prvTrigs};
                if ( defined(my $ctrg = $curTrigs->{$triggerid}) ) {
                    my ($plfk, $clfk) = map __get_trigger_lostfunk, $ptrg, $ctrg;
                    ($plfk == $clfk)
                        ? ()
                        : do { 
                            debug { 'prv_trig=<<%s>>, cur_trig=<<%s>>', __json($ptrg), __json($ctrg) };
                            $ctrg->{'svcpath'} = $ptrg->{'svcpath'};
                            ( [ $triggerid, $ctrg, $clfk ] )
                        }
                } else {
                    ()
                }
            } 1..keys %{$prvTrigs}
        ) {
            debug { 'diffTrigs:', __json( \@diffTrigs ) };
            $redcTrigsDb->write( map @{$_}[0,1], @diffTrigs );
            my $updServices = $slf->doCalcSvcTreeChanges( \@diffTrigs );
            $slf->get_redc_for('services')->write( $updServices )
                ? do {
                    info_ 'Updated %d services state in Redis', scalar(keys %{$updServices});
                    [DONE, undef]
                  }
                : do {
                    error_ my $err = 'Failed to update services in Redis';
                    [undef, $err]
                  }
        } else {
            info { 'There are no triggers that changed its state' };
            [undef, undef]
        }
    } catch {
        error { 'Error when updating triggers state:', $_ };
        return { 'error' => "(catched) $_" };
    } finally {
        $lock->post unless $lock->getvalue > 0;
        $lock->close;
    };
}

sub doCalcSvcTreeChanges {
    my ($slf, $diffTrigs) = @_;
    # @{$diffTrigs} structure:
    # (
    # 	[ triggerid0, trigger_struct0, actual_lostfunk0 ],
    #	[ ... ],
    #   [ triggeridN, trigger_structN, actual_lostfunkN ]
    # )
    my %updRecord;
    my ($updrTrigs, $updrSvcs) = @updRecord{'t','s'} = ({},{});
    my $now_ts = __time(undef);
    my $affectedSvcs = +{ map {
        my ($triggerid, $trg, $curLFK) = @{$_};
        my $svcPath = $trg->{'svcpath'};
        (map {
            $_ => [ $curLFK, $curLFK >= LFK_OK ? $trg->{'lastchange'} : $now_ts ],
        } uniq map pop(@{$_}), @{$svcPath}), # keys +{ map { pop($_)=>1 } @{$svcPath} }),
        map {
            $_ => undef
        } map @{$_}, @{$svcPath}
    } @{$diffTrigs} };
    debug { 'affectedSvcs based on diffTrigs:', __json($affectedSvcs) };
    # %knownLFK is a filtered version of %affectedSvcs containing only serviceid => lostfunk pairs, where lostfunk is defined (i.e. corresponding to triggers)
    my %knownLFK = map { my @t = each %{$affectedSvcs}; defined($t[1]) ? (@t) : () } 1 .. keys %{$affectedSvcs};
    debug { '(initial) knownLFK=%s', __json(\%knownLFK) };
    delete @{$affectedSvcs}{keys %knownLFK};
    my @nodeServiceIds = keys %{$affectedSvcs};
    # Goal is to read all of the affected objects, because in general case we will need to write it back with updated "lostfunk" attribute
    $slf->get_redc_for('services')->read({}, @nodeServiceIds, keys(%knownLFK), sub {
        $affectedSvcs = $_[0];
        for (keys %knownLFK) {
            @{$affectedSvcs->{$_}}{qw/lostfunk updatets/} = @{$knownLFK{$_}};
            $knownLFK{$_} = $knownLFK{$_}[0]
        }
        
#        my %assocHostIds;
        
#        for  ( grep { exists($_->{'zloid'}) && substr($_->{'zloid'}, 0, 1) eq 'h' } @{$affectedSvcs}{@nodeServiceIds} ) {
#            push @{$assocHostIds{substr($_->{'zloid'}, 1)}}, $_->{'serviceid'}
#        }
        
#        if ( %assocHostIds ) {
#            for my $hostInMaint ( grep {is_plain_hashref($_) && $_->{'maintenance_status'}} @{$slf->get_redc_for('hosts')->read(keys %assocHostIds)} ) {
#                $affectedSvcs->{$_}{'lostfunk'} = $knownLFK{$_}[0] = LFK_MAINTENANCE for @{$assocHostIds{$hostInMaint->{'hostid'}}};
#            }
#        }

        # Read (to %knownLFK) lostfunks of all dependent services for the "affected" ones
        if ( my @notAffectedDeps = 
              grep ! exists($affectedSvcs->{$_}), uniq map @{($_->{'dependencies'} || [])}, values %{$affectedSvcs}
        ) {
            debug { 'notAffectedDeps=[', join(', ' => @notAffectedDeps),']' };
            $slf->get_redc_for('services')->read_not_null(@notAffectedDeps, sub {
                @knownLFK{@notAffectedDeps} = map $_->{'lostfunk'}, @{$_[0]}
            });
        }
        
    });
    $slf->get_redc_for('services')->wait_all_responses;
    
    debug { "knownLFK before doRecalcLostFunk():\n%s\naffectedSvcs as passed to doRecalcLostFunk():\n%s", __json(\%knownLFK), __no_deps_json($affectedSvcs) };
    doRecalcLostFunk( $affectedSvcs, \%knownLFK, $_ )
        for grep exists($affectedSvcs->{$_}), keys %{$slf->('servicesOfInterest')};

    debug { 'affectedSvcs after doRecalcLostFunk():', __no_deps_json( $affectedSvcs ) };
    delete @{$affectedSvcs}{
        map { 
            my ($svcid, $v) = each %{$affectedSvcs};
            defined($v) ? () : $svcid
        } 1 .. keys %{$affectedSvcs}
    };
    return $affectedSvcs;
} # <- doCalcSvcTreeChanges()

sub doRecalcLostFunk {
    my ($affectedSvcs, $knownLFK, $svcid) = @_;
    $svcid //= 0;
    debug { 'Recalculating s%s...', $svcid };
    (exists $affectedSvcs->{$svcid} and defined(my $svc = $affectedSvcs->{$svcid})) or do {
        warn_ 'Service #%d not exists or not defined in affected services list', $svcid;
        return
    };
    if ( defined(my $curLFK = $knownLFK->{$svcid}) ) {
        warn_ 'lostfunk for svc#%d is already known (=%s), why do you want to recalculate it?', $svcid, $curLFK;
        return $curLFK
    }
    my ($prvLFK, $svcDeps) = @{$svc}{qw(lostfunk dependencies)};
    # We dont need to know previous lostfunk for services associated with triggers, so... 
    # you cant pass any triggers in $affectedSvcs or you have to define know previous state of the trigger (which is not very informative)
    defined($prvLFK) or logdie { 'WTF, why "lostfunk" is not defined in your service <<%s>> ?', __json($svc) };
    if ( $prvLFK == ($svc->{'lostfunk'} = LFK_LOOP_PROTECT) ) {
        logdie { 'We detected a loop while desc to <<%s>> service subtree. Any further lostfunk calculations is impossible!', $svcid };
        return
    }
    
    $knownLFK->{$svcid} = my $curLFK = do {
    	if ( is_plain_arrayref $svcDeps ) {
            my @depsLFK = grep { $_ >= 0 } map {
                0 + ( 
                    exists($knownLFK->{$_}) 
                        ? ( $knownLFK->{$_} // logdie { "Invalid value for key <<%s>> in knownLFK:\n%s", $_,  __json($knownLFK) } )
                        : ( 
                            doRecalcLostFunk($affectedSvcs, $knownLFK, $_)
                                //
                            do { logdie_('Cant calculate lostfunk for service #%d', $svcid); 0 }
                          )
                )
            } @{$svcDeps};
            
            debug { sprintf('@depsLFK[parsvc=%s,algo=%d]=%s', $svcid, $svc->{'algorithm'}, join(',' => @depsLFK)) };
            @depsLFK
                ? $svc->{'algorithm'} == SVC_ALGO_ONE_FOR_PROBLEM
                     ? max(@depsLFK)			# worst case
                     : sum(@depsLFK) / scalar(@depsLFK) # average value
                : LFK_UNKNOWN
        } else {
	    warn { 'Service #%d has no dependencies, but it is not valid leaf element (not associated with some trigger), so assuming its lostfunk is UNKNOWN',  $svcid };
	    LFK_UNKNOWN
	}
    } + 0;
    
    # Determine whether something really changed in service state after trigger switch
    if ( $curLFK == $prvLFK or abs( $curLFK - $prvLFK ) <= LFK_DELTA_TOLERANCE ) {
        debug { 'LostFunK for service #%d was not changed, so no update needed', $svcid };
        undef $affectedSvcs->{$svcid}
    } else {
        @{$svc}{qw/lostfunk updatets/} = ($curLFK + 0, __time());
        if ( abs( $prvLFK ) <= LFK_DELTA_TOLERANCE and $curLFK > 0 ) {
            debug { 'Service %d switched to fail state [%s => %s]', $svcid, $prvLFK, $curLFK };
            $svc->{'failts'} = __time();
        } elsif ( exists($svc->{'failts'}) and abs( $curLFK ) <= LFK_DELTA_TOLERANCE and $prvLFK > 0 ) {
            debug { 'Service %d switched back to normal state [%s => %s]', $svcid, $prvLFK, $curLFK };
            delete $svc->{'failts'};
        }
    }
    return $curLFK
}



# Function doCalcLostfunk()
# 	Description:
#            Initially calculate lostfunks and maintenance_flag's in the service tree (recursive function)
# 	Parameters:
# 		$svc - parent service object or parent serviceid. $svc==0 by default (i.e. $svc will be global root service if not specified explicitly)
#		$zo  - hash containing IT services objects splited by categories/objectTypes:
#			{'s'} => services
#				...and its "associated objects", i.e.:
#			{'g'} => hostgroups
#			{'h'} => hosts
#			{'t'} => triggers
#		@svcPath - in recursive calls, "services" path from the initial service (which serviceid you use when calling this method) -
# 		           to the direct parent of the current calculated service
sub doCalcLostFunK {
    my ($svc, $zo, $trgs, @svcPath) = @_;
    do {
        error_ "Invalid service:", $json->encode([$svc]);
        return
    } unless 
        defined( $svc )
            and
        ( is_plain_hashref($svc) or (!ref $svc and $svc = $zo->{'s'}{$svc // 0}) )
            and
        defined(my $svcid=$svc->{'serviceid'})
            and
        ( $svc->{'lostfunk'}-- >= LFK_NOT_CALC );
    
    if ( $svc->{'lostfunk'} >= LFK_NOT_CALC ) {
        $svc->{'lostfunk'}++;
        debug { 'IT service already seen: #%d', $svcid };
        return $svc
    }
    
    utf8::decode($svc->{'name'}) if $svcid;
    if (my $triggerid = $svc->{'triggerid'}) {
        my $trg = $zo->{'t'}{$triggerid} or do {
            error_('Anomaly detected: service %d has "triggerid" attr but no associated trigger object found', $svcid);
            return
        };
        $svc->{'lostfunk'} = ( $trgs->{$triggerid} //= __get_trigger_lostfunk( $trg ) );
        $svc->{'failts'} = $trg->{'lastchange'} if $trg->{'value'} and !$trg->{'status'};
        $svc->{'nestd'} = LEAF_SERVICE_NESTD
    } else {
        my $flSvcAssocWithHost = FALSE;
        if ( $svcid and my $zloid=$svc->{'zloid'} ) {
            my ($zoltr, $zoid) = $zloid =~ m/^([${ltrs}])(\d+)$/;
            unless (my $zobj = $zo->{$zoltr}{$zoid}) {
                warn { 'Anomaly detected: service #%d contains invalid zloid %s', $svcid, $zloid };
                delete $svc->{'zloid'};
            } elsif (
                defined(my $earlyLFK = (
                    {
                        't' => sub {
                            warn { 'Anomaly detected: service #%d has no "triggerid" attribute, but it was incorrectly associated with the trigger #%d by name: maybe trigger was deleted?', $svcid, $zoid };
                            LFK_UNKNOWN
                        },
                        'h' => sub {
                                    $flSvcAssocWithHost = TRUE;
                                    return unless $zobj->{'status'} or $zobj->{'maintenance_status'};
                                    $zobj->{'status'}
                                    ? do { 
                                        debug { 'Service #%d associated with disabled host #%d', $svcid, $zoid };
                                        if ( my $deps = __va_array_ref($svc->{'dependencies'}) ) {
                                            __list_deps_to_wipe($zo, $svcid, $deps, my $deps2wipe = {});
                                            if ( %{$deps2wipe->{'s'}} ) {
                                                while (my ($zoltr, $zoids) = each %{$deps2wipe}) {
                                                    debug { 'Removing objects lying under service#%d associated with disabled host #%d: [%s]', $svcid, $zoid, join( ',' => map $zoltr.$_, keys(%{$zoids}) )};
                                                    delete @{$zo->{$zoltr}}{keys %{$zoids}}
                                                }
                                            }
                                            $svc->{'dependencies'} = []
                                        }
                                        LFK_DISABLED
                                      }
                                    : do {
                                        debug { 'Service #%d associated with host #%d which is in maintenance status now', $svcid, $zoid };
                                        $svc->{'maintenance_flag'} = TRUE;
                                        undef
                                      }
                        },
                    }->{$zoltr} // sub {}
                )->()
            )) { # if defined( $earlyLFK ) {
                @{$svc}{qw/lostfunk nestd/} = ($earlyLFK, LEAF_SERVICE_NESTD);
                return $svc
            }
        }
        
        # Recursive descend to calculate depndencies (also applicable to hosts in maintenance: all of its dependencies will be calculated)
        is_plain_arrayref( $svc->{'dependencies'} )
            and 
            my @deps = map { 
                doCalcLostFunK(
                    $zo->{'s'}{$_} // die(sprintf 'service#%d mentioned as dependence for service #%d but it does not exists!', $_, $svcid),
                    $zo, 
                    $trgs,
                    (@svcPath, $svcid)
                ) or die "FATAL: cant calc $_ service: recursive doCalcLostFunK() call returns nothing/undef"
            } @{$svc->{'dependencies'}};
        if ( @deps ) { 		# if this service have dependencies/children services...
            # lost functionality coefficient calculation ->
            my @childLFKs = grep { $_ >= LFK_OK } map $_->{'lostfunk'}, @deps;
            $svc->{'lostfunk'} = 
            @childLFKs
                ? do {
                    my $lostFunK = 
                        $svc->{'algorithm'} == SVC_ALGO_ONE_FOR_PROBLEM
                            ? max(@childLFKs)
                            : sum(@childLFKs) / scalar(@deps);
                    $lostFunK > LFK_DISASTER ? LFK_DISASTER : $lostFunK
                  }
                : do {
                    debug { 'Service #%d has no lfk-calculable deps', $svcid };
                    LFK_UNKNOWN
                  };
            # <- lost functionality coefficient calculation
            
            # maintenance_flag calculation ->
            if (! $flSvcAssocWithHost  # i.e.: "if this service is not associated with the host, because host is a "leaf" in maintenance_flag propagation tree"
                    and
                ( $svc->{'algorithm'} == SVC_ALGO_ONE_FOR_PROBLEM and any { $_->{'maintenance_flag'} } @deps
                    or
                 all { $_->{'maintenance_flag'} } @deps )
            ) {
                $svc->{'maintenance_flag'} = TRUE
            }
            # <- maintenance_flag calculation
            
            # maximum level of dependencies nesting a.k.a "nestd" calculation ->
            $svc->{'nestd'} = max( map { $_->{'nestd'} // LEAF_SERVICE_NESTD } @deps, {'nestd' => LEAF_SERVICE_NESTD} ) + 1;
            # <- maximum level of dependencies nesting a.k.a "nestd" calculation
            
            if (my $failTS = min( map $_->{'failts'}, grep defined($_->{'failts'}), @deps )) {
                $svc->{'failts'} = $failTS
            }
            
        } else {
            debug { 'Service #%d is not associated with trigger but has no any valid deps', $svcid, $json->encode($svc) };
            $svc->{'lostfunk'} = LFK_UNKNOWN;
            $svc->{'nestd'   } = LEAF_SERVICE_NESTD;
        }
    }
    return $svc
}

our $AUTOLOAD;
sub AUTOLOAD {
    my $iam = shift;
    my ($pkg, $what2do, $par) = $AUTOLOAD =~ m/^(.*::)([gs]et)_(.+)$/
        or confess 'No such method <<'.$AUTOLOAD.'>>';
    no strict 'refs';
    unless (exists &{$pkg . 'set_' . $par}) {
        *{$pkg . 'set_' . $par}=sub {
            my ($slf, $val) = @_;
            $slf->( +{$par => $val} );
        }
    }    
    unless (exists &{$pkg.'get_'.$par}) {
        *{$pkg . 'get_' . $par} = sub {
            $_[0]->( $par );
        }
    }
    use strict 'refs';
    goto &{$what2do . '_' . $par};
}

sub DESTROY {
    my $slf = shift;
    $slf->('dbhSource')->disconnect;
    
    my $locks = $slf->('semlocks');    
    return 1 unless is_plain_hashref($locks) and %{$locks};
    debug_ 'Removing ZObj semlocks';
    for my $lock (grep { ref($_) eq 'POSIX::RT::Semaphore::Named' } values %{$locks}) {
        $lock->post unless $lock->getvalue > 0;
        $lock->close;
    }
}

sub __trace_svc_path {
    my ($svcs, $svcid, %path) = @_;
    return ( [$svcid] ) 
        unless my @pars = eval { @{$svcs->{$svcid}{'parents'}} };
    return ( map [@{$_}, $svcid], 
                map { 
                    exists $path{$_} and die "$_ cant be parent for $svcid"; 
                    __trace_svc_path($svcs, $_, (%path, $svcid=>1)) 
                }
            @pars );
} # <- __trace_svc_path()

sub __purge_svcs {
    my ($hshSvcs, @svcids) = @_;
    for my $svc (grep is_plain_hashref($_), delete @{$hshSvcs}{@svcids}) {
        next unless is_plain_arrayref( $svc->{'parents'} ) and my @pars = @{$svc->{'parents'}};
        my $svcid = $svc->{'serviceid'} // next;
        delete @{$_}{$svcid} for grep is_plain_hashref($_), map $hshSvcs->{$_}{'dependencies'}, @pars;
    }
    return 1
} # <- __purge_svcs()

sub __json {
    $json->pretty if $_[1];
    $json->encode( ref $_[0] ? $_[0] : [$_[0]] );
}

sub __no_deps_json {
    my $h=shift;
    my $fn=__fq_func_name();
    logdie { 'You must pass hashref or arrayref to %s, but this is %s', $fn, Dumper([$h]) } unless ref $h and ref($h) =~ /^(?:ARRAY|HASH)$/;
    JSON::XS->new->encode({
        map {
            my ($k, $v) = each %{$h};
            $k => 
                is_hashref($v)
                    ? +{
                        map { $_ => $v->{$_} }
                            grep { $_ ne 'dependencies' }
                                keys %{$v}
                      }
                    : $v
        } 1 .. keys %{$h}
    });
}

sub __fq_func_name {
    return scalar((caller(1))[3])
}

sub __check_table_exists {
    shift if ref($_[0]) eq __PACKAGE__;
    my ($dbh,$tableName)=@_;
    return $dbh->selectall_arrayref(qq(select count(1) from information_schema.tables where table_name='${tableName}' and table_schema=database()))->[0][0];
}

sub __time {
    state $now;
    -(@_) 			     # was (any, i.e. undef too) parameters passed to me?
        ? ( $now = $_[0] // time() ) # if any parameters was passed, including undef - update cached time
        : ( $now //= time() )	     # returns cached timestamp (optionally setting it on first call when state variable $now is still undefined)
}

sub __hp_timer {
    state $ts = Time::HiRes::time();;
    defined(wantarray)
        ? ( Time::HiRes::time() - $ts )  # "any valuable" context (meaningless - scalar or array)
        : ( $ts = Time::HiRes::time() ); # void context
}

sub __get_sth {
    ref(my $sth = eval { $_[0]->('st')->{$_[1]} }) eq 'DBI::st'
        or logdie_( '%s statement unknown or was not prepared', $_[1] );
    return $sth
}

sub __get_dbh {
    state $dbTypeRel = {
        'mysql' => {
            'dsn_templ' => 'dbi:mysql:host=%s;database=%s',
            'add_options' => +{
                'mysql_enable_utf8' 	=> TRUE,
                'mysql_auto_reconnect'  => TRUE,
            },
            'after_connect_do' => ['SET NAMES utf8'],
        },
        'postgresql' => {
            'dsn_templ' => 'dbi:Pg:host=%s;dbname=%s',
#            'add_options' => +{
#            },
            'after_connect_do' => [q<SET CLIENT_ENCODING TO 'UTF8'>],
        },
    };
    shift if ref($_[0]) eq __PACKAGE__;
    (caller)[0] eq __PACKAGE__
        or confess  'You should not use this function outside package ' . __PACKAGE__;
    my ($dbConn, $dbName, %execAfterOpts) = @_;
    # dbc means "Database Settings" :)
    my $dbc = $dbConn->{ $dbName } or confess "No connectors for $dbName defined"; 
    for ( my $dbType = $dbc->{'type'} ? lc($dbc->{'type'}) : 'mysql' ) 
    {
        when ( /sql$/ ) {
            my $dbr = $dbTypeRel->{$dbType};
            my $dbh = $dbc->{'dbh'} = DBI->connect(
                sprintf($dbr->{'dsn_templ'}, @{$dbc}{qw/host database/}),
                @{$dbc}{'user','password'}, 
                +{
                    'RaiseError'		=> YES, 
                    exists($dbc->{'autocommit'})
                        ? ( 'AutoCommit' => $dbc->{'autocommit'} )
                        : (),
                    exists($dbr->{'add_options'})
                        ? %{$dbr->{'add_options'}} 
                        : ()
                }
            );
            if (exists $dbr->{'after_connect_do'}) {
                $dbh->do($_) for @{$dbr->{'after_connect_do'}};
            }
        }
        when ( /redis/ ) {            
            $dbc->{'dbh'} = RedC->new( 
                'server'	=> join( ':' => @{$dbc}{'host','port'} ),
                'index'		=> $dbc->{'dbnum'},
                'name'		=> $dbName,
            );
        }
        default {
            confess 'Unknown dbtype: ' . $dbType;
        }
    }
    my $exec_after = $dbc->{'exec_after'} or return $dbc->{'dbh'}; 
    my $dbh = $dbc->{'dbh'};
    given ( ref $exec_after ) {
        when ( 'ARRAY' ) {
            $_->($dbh, \%execAfterOpts) for @{$exec_after};
        }
        when ( 'HASH' ) {
            $_->[1]->( $_->[0] => $dbh, \%execAfterOpts ) for map [each %{$exec_after}], 1 .. keys %{$exec_after};
        }
        when ( 'CODE' ) {
            $exec_after->($dbh, \%execAfterOpts)
        }
        default {
            confess sprintf 'Please check "exec_after" database %s connection setting: it contains something totaly useless', $dbName
        }
    }
    return $dbh
}

sub __list_deps_to_wipe {
    my ($zo, $parid, $svcids, $alreadySeen) = @_;
    my $svcAlreadySeen = ($alreadySeen->{'s'} //= {});
    my $c = 0;
    for my $svc ( @{$zo->{'s'}}{grep !exists($svcAlreadySeen->{$_}), @{$svcids}} ) {
        my $svcid = $svc->{'serviceid'};
        my ($pars, $deps) = @{$svc}{'parents','dependencies'};
        if ( $#{$pars} > 0 ) {
            for my $i (0..$#{$pars}) {
                splice($pars, $i, 1, ()), last if $pars->[$i] == $parid;
            }
        } else {
            $svcAlreadySeen->{$svcid} = 1;
            if (my $zloid = $svc->{'zloid'}) {
                $alreadySeen->{substr($zloid, 0, 1)}{substr($zloid, 1)} //= 1
            }
            is_plain_arrayref($deps) and $#{$deps} >= 0 
                and __list_deps_to_wipe($zo, $svcid, $deps, $alreadySeen)
        }
    }
}

sub __get_trigger_lostfunk {
    my $trg = $_[0] // $_ // die 'Cant calculate lostfunk on undefined trigger object';
    is_plain_hashref($trg) or die 'Trigger object must be represented as a hash reference';
    $trg->{'status'}
        ? LFK_DISABLED
        : $trg->{'state'}
            ? LFK_UNKNOWN
            : $trg->{'value'} && ($trg->{'priority'} > TRIG_PRIO_INFO)
                ? ($trg->{'maintenance'} ? -1 : 1) * ($trg->{'priority'} - TRIG_PRIO_INFO) * RN_VAL_TRIG_PRIO
                : LFK_OK
}

sub __va_array_ref {
    is_plain_arrayref($_[0]) && $#{$_[0]} >= 0 ? $_[0] : undef
}

sub __flatn {
    map is_plain_arrayref($_) ? @{$_} : $_, @_
}

sub __dbr_listagg {
    my ($slf, $field) = @_;
    given ( $slf->('dbhSource')->get_info(SQL_DBMS_NAME) ) {
        sprintf(q<STRING_AGG(CAST(%s AS text), ',')>, $field) when /^Postgre/;
        sprintf(q<GROUP_CONCAT(%s SEPARATOR ',')>, $field) when /^My/;
    }
}

sub __dbr_ternary {
    my ($slf, $cond, @fields) = @_;
    given ( $slf->('dbhSource')->get_info(SQL_DBMS_NAME) ) {
        sprintf('IF(%s, %s, %s)', $cond, @fields[0,1]) when /^My/;
        sprintf(q<CASE WHEN (%s) THEN %s ELSE %s END>, $cond, @fields[0,1]) when /^Postgre/;
    }
}

1;
