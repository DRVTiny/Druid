#!/usr/bin/perl
use 5.16.1;
use strict;
use warnings;
use constant {
    ZAPI_CONFIG			=> '/etc/zabbix/api/setenv.conf',
    BCST_NAME			=> 'zabbix',
    BCST_CHANNEL		=> 'maint_status_changes',
    FL_REMOVE_SEM_IF_EXISTS 	=> 2,
    DFLT_BASE_DIR		=> '.',
    IMMEDIATELY			=> 0,
    MIN_PERIOD_BTW_RELOADS	=> 120, # sec.
    BASE_DIR			=> '.',
    DFLT_CACHE_RELOAD_INTERVAL	=> 3 * 3600, # sec.
    DFLT_TRIG_UPDATE_POLICY	=> '10:120', # after:interval (sec.)
    DFLT_HOST_UPDATE_POLICY	=> '20:300', # -//-
    DFLT_PROD_PID_FILE		=> '/var/run/druid/calc-engine.pid',
};
use Data::Dumper qw(Dumper);

use FindBin;
use lib (
  $FindBin::RealBin . '/../lib/app', # first priority
  qw</opt/Perl5/libs /usr/local/share/perl5 /usr/local/lib64/perl5>,
  $FindBin::RealBin . '/../lib/cmn', # least priority
);

use File::Basename qw(dirname);
use File::Path qw(mkpath);
use Getopt::Long::Descriptive;

use EV;
use AnyEvent;
use Redis::BCStation;
use Tag::DeCoder;
use File::SafeOps::PID;
use Log::Log4perl::KISS;

sub ctx_check(&$);

my $slfName = $0 =~ s%(?:^.*/|\.[^.]+$)%%gr;
my $dfltPIDFile = 
 ($ENV{'DRUID_MODE'} && $ENV{'DRUID_MODE'} eq 'development')
    ? $FindBin::RealBin . '/../run/calc-engine.pid'
    : DFLT_PROD_PID_FILE;
my $dfltZapiConfig = $ENV{'ZAPI_CONFIG'} // ZAPI_CONFIG;
my ($opt, $usage) = describe_options(
        '%c %o',
        [ 'test|t', 				'Do it in the "test" mode. Will be used "safe" environment which do not affect production service operations' ],
        [ 'setenv-conf|e=s',			'Path to Zabbix API "setenv" file, default: ' . $dfltZapiConfig, {'default' => $dfltZapiConfig}],
        [ 'base-dir|d=s',			'Base directory path', {'default' => DFLT_BASE_DIR} ],
        [ 'pid-file|p=s',			sprintf('PID file path (default: %s)', $dfltPIDFile), {'default' => $dfltPIDFile } ],
        [ 'sock-path|S=s', 			'Path to UNIX socket where we will accept some useful runtime commands' ],
        [ 'log-file|L=s',			'Log file path, specify "-" for STDERR (mutualy exclusive with the "-l|--logconf" option)' ],
        [ 'update-triggers-interval|U=s', 	'Update triggers interval in format: [after:]period, where after and period/interval value specified in seconds', {'default' => DFLT_TRIG_UPDATE_POLICY} ],
        [ 'update-hosts-interval|H=s',  	'Update hosts maintenance status interval in format: [after:]period, where after and period/interval value specified in seconds', {'default' => DFLT_HOST_UPDATE_POLICY} ],
        [ 'full-reload-interval|F=s',		'Full reload every N seconds (default: '.DFLT_CACHE_RELOAD_INTERVAL.')', {'default'=>DFLT_CACHE_RELOAD_INTERVAL} ],
        [ 'run-after-reload|r=s', 		'Command to run after full reload' ],
        [ 'root-services|s=s',			'Comma-delimited root services list' ],
        [ 'redis-bc-station|B=s', 		'Redis broadcasting station name (if not specified, "cache2up" or "cache2up_tst" will be choosen depending on effective operation mode)'],
        [],
        [ 'help',       			'Print this helpful "usage" message and exit' ],
);
print('Usage: ' . $usage->text), exit if $opt->help;
$ENV{ZAPI_CONFIG} = $opt->setenv_conf;

require Druid::CalcEngine or die 'failed to load Druid::CalcEngine package';
$opt->full_reload_interval =~ /^\d+$/ or die 'Invalid full reload interval: it must be numeric';

my $onFullReloadExec = (grep { defined and -f $_ and -r $_ and -x $_ } $opt->run_after_reload) ? $opt->run_after_reload : undef;

my $sfxWhenDebug = $opt->test ? '_tst' : '';

my $fhPID = File::SafeOps::PID->new(
    mkpath_for_file($opt->pid_file // sprintf('%s/run/%s%s.pid', $opt->base_dir, $slfName, $sfxWhenDebug))
);

log_open($opt->log_file) if defined $opt->log_file;

my $zo = Druid::CalcEngine->new(
    defined($opt->root_services) ? ('root_services' => [split /,/ => $opt->root_services]) : (),
    'encoder' => 'MP'
);

$zo->reloadCache2;

my %how2 = (
    'update_triggers' 	=> sub {
        $zo->actualizeTrigValues(flags => FL_REMOVE_SEM_IF_EXISTS);
    },
    'update_hosts'	=> sub {
        $zo->actualizeHosts();
    },
);

my $cv = AnyEvent->condvar;
my %aeh = map {
    my $what2do = 'update_' . $_;
    $opt->{$what2do . '_interval'} =~ /^(?:(\d+):)?(\d+)$/
        or logdie_('Invalid policy for %s. Must be: "[after:]period", where "after" and "period" both are numerical', $what2do);
    ($what2do => AnyEvent->timer(
        'after'		=> $1 // IMMEDIATELY,
        'interval'	=> $2,
        'cb'		=> $how2{$what2do}
    ))
} qw/triggers hosts/;
# TODO: << logdie_('Any update period cant be >= full reload interval (%s)', $opt->full_reload_interval) if grep {$_->{'updateInterval'} >= $opt->full_reload_interval} values %timers; >>

Redis::BCStation->new(BCST_NAME)->subscribe(BCST_CHANNEL, sub {
    my $host = decodeByTag($_[0]);
    debug { 'affectedHost=', Dumper($host) };
    my $res = $zo->actualizeMaintFlag( @{$host}{qw<hostid maintenance_status>} );
    debug { 'actualizeMaintFlag(%s,%s) = ' . Dumper([$res]) } @{$host}{qw<hostid maintenance_status>};
});

$cv->recv;

########################## FOOTER ####################################
sub ctx_check (&$) {
    local $_ = $_[1];
    $_[0]->()
}

sub mkpath_for_file {
    my $d = dirname($_[0]);
    mkpath($d) unless -d $d;
    $_[0];
}
