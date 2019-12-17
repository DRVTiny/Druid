#!/usr/bin/perl -CDA
use 5.16.1;
use strict;
use warnings;
use utf8;
use constant {
    ZAPI_CONFIG              => '/etc/zabbix/api/setenv.conf',
    TIMEZONE                 => 'MSK',
    DEFAULT_GOOD_SLA         => '99.05',
    SLA_ALGO_ALL_FOR_PROBLEM =>  2,
    DFLT_DB_TYPE             => 'mysql',
};
use Data::Dumper;

use FindBin;
use lib (
  $FindBin::RealBin . '/../lib/app', # first priority
  qw</opt/Perl5/libs /usr/local/share/perl5 /usr/local/lib64/perl5>,
  $FindBin::RealBin . '/../lib/cmn', # least priority
);

use Carp qw(croak);
use Ref::Util qw(is_plain_hashref is_plain_arrayref is_hashref is_arrayref);
use Scalar::Util::LooksLikeNumber qw(looks_like_number);
use Config::ShellStyle;
use Monitoring::Zabipi qw(zbx zbx_last_err zbx_api_url zbx_get_dbhandle);
use Monitoring::Zabipi::ITServices;

use Getopt::Std qw(getopts);
use JSON::XS;

no warnings;

my %SETENV = %{read_config ZAPI_CONFIG};

my $apiPars        = { 'wildcards' => 'true' };
my $firstarg = shift;
if ($firstarg eq '-x') {
    $apiPars->{'debug'}  = 1;
    $apiPars->{'pretty'} = 1;
} else {
    unshift @ARGV, $firstarg;
}
die 'You must specify ZBX_HOST or ZBX_URL in your config ' . ZAPI_CONFIG
    unless my $zbxConnectTo = $SETENV{'ZBX_HOST'} || $SETENV{'ZBX_URL'};
die 'Cant initialize API, check connecton parameters (ZBX_HOST or ZBX_URL. Connect='
    . $zbxConnectTo
    . ') in your config '
    . ZAPI_CONFIG
    unless Monitoring::Zabipi->new($zbxConnectTo, $apiPars);
zbx('auth', @SETENV{'ZBX_LOGIN', 'ZBX_PASS'})
    || die 'I cant authorize you on ', $zbxConnectTo,
    ". Check your credentials and run this script with the first key '-x' to know why this happens exactly\n";

my $itsvc = Monitoring::Zabipi::ITServices->new();

#
#die Dumper [ scalar $itsvc->get_svc_parents(37343, 1) ];
# Your code goes here ->
# For example, you may uncomment this line to get "Zabbix server" on STDOUT:
my %doSmth;
%doSmth = (
    'create' => {
        'func' => sub {
            my ($svcName, %opts) = @_;
            die 'You must specify service name' unless $svcName;
            my ( $parentid ) = 
            defined( $opts{'-p'} )
                ? $opts{'-p'} =~ m/[^\d]/
                    ? ( map $_->{'serviceid'}, @{zbx('service.get', {'search' => {'name' => $opts{'-p'}}, 'output' => ['serviceid']})} )
                    : ( $opts{'-p'} )
                : undef;
            my $show_flags = ($opts{'-f'} =~ m/^\d{1,3}$/ and $opts{'-f'} < 256) ? $opts{'-f'} : 0;
            my $algo
                = (exists($opts{'-a'}) and $opts{'-a'} !~ m/[^\d]/ and $opts{'-a'} >= 0 and $opts{'-a'} <= 2)
                ? $opts{'-a'}
                : SLA_ALGO_ALL_FOR_PROBLEM;
            $itsvc->create(
                {   'name'      => $svcName,
                    'algorithm' => $algo,
                    ($parentid ? ('parentid' => $parentid) : ()),
                    'goodsla'   => DEFAULT_GOOD_SLA,
                    'showsla'   => $show_flags,
                    'sortorder' => 0,
                }
            );
        }, # <- create.func
    },
    'get' => {
        'func' => sub {
            my ($svc, %opts) = @_;
            my @serviceids = looks_like_number($svc) ? ( $svc ) : $itsvc->get_sid_by_name($svc)
                or return {'error' => 'No such ITService'};
            
            my @getPars = (
                ((!exists($opts{'p'}) or $opts{'p'} =~ m/[^\d]/) ? () : ('parentids' => $opts{'p'})),
                ('serviceids' => \@serviceids),
                'output'       => [qw(name triggerid showsla goodsla sortorder algorithm)],
                'selectParent' => 'extend',
            );
            my ($zobj) = @{zbx('service.get', {@getPars})}
                or return {'error' => 'ITService not found'};

            if ( is_plain_arrayref($zobj->{'parent'}) ) {
                $zobj->{'parent'} = {'serviceid' => 0, 'name' => '#'};
            } elsif (exists $zobj->{'parent'}{'triggerid'}) {
                delete $zobj->{'parent'}{'triggerid'};
            }

            if (exists($zobj->{'triggerid'}) and $zobj->{'triggerid'}) {
                ($zobj->{'trigger'}) = @{
                    zbx('trigger.get',
                        {   'triggerids'        => $zobj->{'triggerid'},
                            'expandDescription' => 1,
                            'expandExpression'  => 1,
                            'output'            => [qw(description expression value status state)],
                            'selectHosts'       => ['name', 'host']
                        }
                    )
                };
            } else {
                delete $zobj->{'triggerid'} if exists $zobj->{'triggerid'};
                if (
                    !exists($opts{'C'}) and my @children = eval {
                        map {utf8::decode($_->{'name'}); delete $_->{'triggerid'} unless $_->{'triggerid'}; $_}
                            @{getITServiceChildren($zobj->{'serviceid'})};
                    }
                ) {
                    $zobj->{'children'} = \@children;
                }
            }
            $itsvc->add_zo_attrs($zobj, 1);
            return $zobj;
        },
        'opts' => 'pC',
    },
    'mv' => {
        'func' => sub {
            scalar(@_) == 2 or die 'you must provide me: SERVICEID_TO_BE_MOVED SERVICEID_WHERE_TO_MOVE';
            $itsvc->move(
                map looks_like_number($_) ? ( $_ ) : $itsvc->get_sid_by_name( $_ ) , @_
            );
        },
    },
    'rm' => {
        'func' => sub {
            my @svcids = map looks_like_number($_) ? $_ : $itsvc->get_sid_by_name($_), @_ 
                or die 'Wrong services passed to me';
            [ map $itsvc->delete($_), @svcids ]
        }
    },
    'rename' => {
        'func' => sub {
            my ($oldName, $newName) = @_;
            $itsvc->rename($oldName, $newName);
        },
    },
    'ln' => {
        'func' => sub {
            my @svcids = map $itsvc->get_sid_by_name($_), @_
                or die 'Wrong services was passed to me';
            $itsvc->create_soft_links(@svcids);
        },
    },
    'ls' => {
        'func' => sub {
            for my $svc (scalar(@_) ? @_ : (0)) {
                my ($svcid, $svcName) = 
                $svc =~ m/[^\d]/
                    ? do {
                        my ($svcid) = $itsvc->get_sid_by_name($svc);
                        unless (defined $svcid) {
                            say STDERR "Cant find service identified as $svc";
                            next
                        }
                        ($svcid, $svc)
                      }
                    : ($svc, $itsvc->get_name_by_sid($svc));
                say "=> ${svcName} [${svcid}]";
                my @svcChildren = eval { @{$itsvc->get_children($svcid)}};
                my %trigid2name;
                if (my @trigids = map $_->{'triggerid'}, grep defined $_->{'triggerid'}, @svcChildren) {
                    %trigid2name = map {$_->{'triggerid'} => $_} @{
                        zbx('trigger.get',
                            {'triggerids' => \@trigids, 'expandDescription' => 0, 'output' => ['description', 'value']})
                    };
                }

                say @svcChildren
                    ? join(
                    "\n",
                    sort {lc($a) cmp lc($b)} map {
                        sprintf('%s [%d]',
                            $_->{'triggerid'}
                            ? join('| ' => @{$trigid2name{$_->{'triggerid'}}}{'value', 'description'})
                            : $_->{'name'},
                            $_->{'serviceid'})
                    } @svcChildren
                    )
                    : '<empty>';
            }
        },
    },
    'unlink' => {
        'func' => sub {
            &{$itsvc->can('unlink')}
        },
    },
    'algo' => {
        'func' => sub {
            my ($svc, $algo) = @_;
            $itsvc->set_algo($svc, $algo);
        },
    },
    'assoc' => {
        'func' => sub {
            my $zobjid         = pop @_;
            my @services2assoc = @_;
            my $svcids =
                @services2assoc == 1 && looks_like_number($services2assoc[0])
                    ? \@services2assoc
                    : [ map $itsvc->get_sid_by_name($_), @services2assoc ];
            my (@err, @assoc);
            for my $serviceid ( @{$svcids} ) {
                my $rslt = $itsvc->associate($serviceid, $zobjid);
                if ( is_plain_hashref($rslt) and exists($rslt->{'error'}) ) {
                    push @err,
                        sprintf('Cant associate ITService %d with Zabbix object %s. Reason: %s',
                        $serviceid, $zobjid, $rslt->{'error'});
                } else {
                    push @assoc, $serviceid;
                }
            }
            +{
                @assoc 
                    ? ('associated' => {'zobj' => $zobjid, 'services' => \@assoc}) 
                    : (), 
                @err
                    ? ('errors' => \@err)
                    : ()
            };
        },
    },
    'deassoc' => {
        'func' => sub {
            my $rslt = $itsvc->deassoc(shift);
            croak 'Cant deassociate ITService: ' . $rslt->{'error'} if exists($rslt->{'error'});
            return $rslt->{'result'};
        },
    },
    'show' => {
        'func' => sub {
            my $subCmd = shift;
            for ($subCmd) {
                when (/associated/) {
                    my $zobjid = shift;
                    return $itsvc->get_associated_svc( $zobjid );
                }
                default {
                    return {'error' => 'No such subcommand ' . $subCmd};
                }
            }
        },
    },
    'help' => {
        'func' => sub {
            my $topic = shift;
            unless ($topic) {
                my $cmdlist = join("\n\t" => '', sort keys %doSmth), "\n";
                printf <<'EOUSAGE', $0, $cmdlist;
Usage:
 %s <COMMAND> [ARGUMENTS]
Where possible COMMANDs are:%s
EOUSAGE
            }
        },
    },
);

my $action = shift || 'help';
die 'No such action <<' . $action . '>>' unless my $hndl = $doSmth{$action};

my %act = ('args' => [], 'pars' => {});

if ($hndl->{'opts'} and @ARGV) {
    push @{$act{'args'}}, shift until (!@ARGV or substr($ARGV[0], 0, 1) eq '-');
    getopts($hndl->{'opts'}, $act{'pars'});
} elsif (!$hndl->{'opts'}) {
    $act{'args'} = \@ARGV;
}

my $res = $hndl->{'func'}->(@{$act{'args'}}, %{$act{'pars'}});
print JSON::XS->new->pretty(1)->encode(ref($res) ? $res : {'result' => $res});

END {
    zbx('user.logout', []) if zbx_api_url();
}
