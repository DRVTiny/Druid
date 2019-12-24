package ZAPI;
use 5.16.1;
use strict;
use warnings;
use boolean;
our $VERSION = 0.5.0;

use constant { YES => 1, NO => 0 };
use enum qw(DBH ZENV ZVERSION DBTYPE DBHCONF GET_LDBH);
use constant ZAPI_CONFIG	=>	'/etc/zabbix/api/setenv.conf';
use constant DFLT_DB_ENGINE	=>	'mysql';
use constant SQL_DBMS_NAME	=>    	17;
use constant DFLT_DBH_CLASS	=>	'DBIx::RetryOverDisconnects';
use constant DFLT_ZBX_VERSION	=> 	'40200';

use Try::Tiny;
use Ref::Util qw(is_hashref is_arrayref is_scalarref is_plain_arrayref is_plain_hashref is_coderef is_plain_coderef);
use List::Util qw(first);
use Config::ShellStyle;
use Carp qw(confess);
use Data::Dumper;

my %acptDbhClasses = (
    'DBI::db' => +{
        'load_class' => 'DBI',
        'constructor' => 'connect',
        'get_ldbh' => sub { 
            $_[0]->ping or $_[0] = $_[0]->clone;
            $_[0]
        },
    },
    'DBIx::Connector' => +{
        'get_ldbh' => sub { $_[0]->dbh },
        'tune' => sub { $_[0]->mode('fixup') },        
    },
    'DBIx::RetryOverDisconnects' => +{
        'constructor' => 'connect',
        'get_ldbh' => sub { $_[0] },
        'append2options' => {
            'ReconnectRetries' 	=> 20,
            'ReconnectInterval' => 1,
            'ReconnectTimeout'  => 10,
        },
        'tune' => sub {
            $_[0]->set_callback('afterReconnect' => sub {
                say STDERR 'Database was disconnected, but DBIx::RetryOverDisconnects reconnected succesfully'
            })
        }
    },
    'DBIx::SQLEngine' => +{
        'get_ldbh' => sub { $_[0]->get_dbh }
     },
);

sub new {
    my ($class, $envPathOrRef, $dbhClass) = @_; 
    state $dbPropsByType = {
        'mysql' => {
            'dsnTemplate' => 'dbi:mysql:host=%s;database=%s',
            'append2options' => {
                'mysql_enable_utf8' 	=> YES,
                'mysql_auto_reconnect'  => YES,
            },
            'after_connect_do' => [ 'SET NAMES utf8',
                                    'SET SESSION group_concat_max_len=@@max_allowed_packet' ],
        },
        'postgresql' => {
            'dsnTemplate' => 'dbi:Pg:host=%s;dbname=%s',
            'append2options' => {
                'pg_server_prepare' => NO,
            },
            'after_connect_do' => [ q<SET CLIENT_ENCODING TO 'UTF8'> ],
        },
    };
    my $zenv = 
        is_hashref($envPathOrRef) 
            ? $envPathOrRef
            : read_config($envPathOrRef // $ENV{'ZAPI_CONFIG'} || ZAPI_CONFIG);
    my $dbType = lc($zenv->{'DB_TYPE'} // DFLT_DB_ENGINE);
    my $dbProps = $dbPropsByType->{$dbType} // die 'dont know how to work with database type ' . $dbType;
    my $dbhConf = $acptDbhClasses{$dbhClass //= $zenv->{'DB_PERL_PKG'} // DFLT_DBH_CLASS};
    $dbhConf or die "dbh class $dbhClass is not acceptable here, use one of: " . join(', ' => keys %acptDbhClasses) . ' instead';
    my $loadClass = $dbhConf->{'load_class'} // $dbhClass;
    $INC{($loadClass =~ s%::%/%gr) . '.pm'} 
        or eval "require $loadClass"
            or die "Can't load $dbhClass: failed to require it's loader lass <<$loadClass>>: $@";
    my $crInitDBH = \&{$loadClass . '::' . ($dbhConf->{'constructor'} // 'new')};
    my $edbh = $crInitDBH->($loadClass =>
    # Will use DB_CONN_OPTIONS(is arrayref) OR, if DB_CONN_OPTIONS undefined or is not arrayref, - use the result of sub {[]}->()
        @{iif_arrayref($zenv->{'DB_CONN_OPTIONS'}, sub { [
            sprintf( $dbProps->{'dsnTemplate'}, @{$zenv}{qw/DB_HOST DB_NAME/} ),
            first_of($zenv, qw/:DB_ USER LOGIN/),
            first_of($zenv, qw/:DB_ PASS PASSWORD/),            
        ]})},
        +{
            'RaiseError' => true,
            (map { exists($_->{'append2options'}) && is_hashref($_->{'append2options'}) ? %{$_->{'append2options'}} : () } $dbProps, $dbhConf),
            exists $dbProps->{'after_connect_do'}
            ?
                ('Callbacks' => {
                    'connected' => sub {
                        my $ldbh = shift;
                        $ldbh->do($_) or die($ldbh->errstr) for is_plain_arrayref($dbProps->{'after_connect_do'}) ? @{$dbProps->{'after_connect_do'}} : ($dbProps->{'after_connect_do'});
                        return
                    }
                })
            : ()
        }
    );
    if ( $dbhConf->{'tune'} ) {
        $_->($edbh) for @{is_plain_arrayref($dbhConf->{'tune'}) ? $dbhConf->{'tune'} : [$dbhConf->{'tune'}]}
    }
    my $crGetLDBH =
        is_coderef( $dbhConf->{'get_ldbh'} )
            ? sub { $dbhConf->{'get_ldbh'}->($edbh) }
            : $edbh->isa('DBI::db')
                ? sub { $edbh }
                : die  "class $dbhClass could not provide low-level database handler";
    my $ldbh = $crGetLDBH->();
    my $crGetInfo = $ldbh->can('get_info') or die "low-level dbh provided by ${dbhClass} does not support method get_info()"; 
    my $zbxVersion = $zenv->{'DB_RESTRICTED'} ? DFLT_ZBX_VERSION : $ldbh->selectall_arrayref('SELECT mandatory FROM dbversion')->[0][0];
    bless [$edbh, $zenv, $zbxVersion, lc($crGetInfo->($ldbh, SQL_DBMS_NAME)), $dbhConf, $crGetLDBH] => (ref($class) || $class)
}

sub fixed_table_name {
# my ($self, $table_name) = @_;
  state $cnvZTableNames = {
    'groups' => sub {
      substr( $_[0][ZVERSION], 0, 1 ) >= 4 ? \'hstgrp' : \$_[1]
    }    
  };
  &{$cnvZTableNames->{$_[1]} // sub { \$_[1] }}
}

sub dbh {	$_[0][DBH] 	}
sub ldbh {	$_[0][GET_LDBH]->() }
sub zenv {	$_[0][ZENV] 	}
sub dbname {  	$_[0][ZENV]{'DB_NAME'} }
sub zversion {	$_[0][ZVERSION] }
sub dbtype {	$_[0][DBTYPE] }

sub first_of {
    my $h = shift;
    if ( $#_ > 0 and length($_[0]) > 1 and substr($_[0], 0, 1) eq ':' ) {
        my $pfx = substr($_[0], 1);
        exists($h->{$_}) and return($h->{$_}) for map $pfx . $_, @_[1..$#_]
    } else {
        exists($h->{$_}) and return($h->{$_}) for @_
    }
    confess 'no such elements in hash found'
}

sub iif_arrayref {
    defined($_[0]) && is_arrayref($_[0]) && @{$_[0]}
        ? $_[0]
        : &{$_[1]}
}

sub __dbi_selectall_arrayref {
  local $SIG{__DIE__} = sub { confess @_ };
  is_hashref($_[2])
    ? &{$_[0]->can('selectall_arrayref')}
    : $_[0]->selectall_arrayref($_[1], {Slice => +{}}, defined($_[2]) ? is_arrayref($_[2]) ? @{$_[2]} : @_[2..$#_] : ())
}

sub selectall_arrayref {
    state $switch_on_dbh_class = [
        [qr/^DBI::db$/ => sub { 
            my $dbh = $_[0]->ldbh;
            unshift @_, shift->[DBH];
            &__dbi_selectall_arrayref
        }],
        [qr/^DBIx::Connector$/ => sub {
            my $dbh = shift->[DBH];
            my $args = \@_;
            $dbh->run('fixup' => sub {
#                printf "ZAPI: running __dbi_selectall_arrayref in fixup mode handler %s => %s\n", ref($_), Dumper($args);
                __dbi_selectall_arrayref($_ => @{$args})
            });
            
        }],
        [qr/^DBIx::RetryOverDisconnects/ => sub {
            unshift @_, shift->[DBH];
            &__dbi_selectall_arrayref
        }],
        [qr/^DBIx::SQLEngine(?:$|::)/ => sub {
            my @args = ($_[1], [], 'fetchall_hashref');
            defined($_[2])
                ? is_arrayref($_[2])
                    ? ( $args[1] = $_[2] )
                    : is_hashref($_[2])
                        ? do { 
                            $args[1] = is_arrayref($_[3]) ? $_[3] : [ @_[3..$#_] ];
                            is_hashref($_[2]{'Slice'}) or $args[2] = 'fetchall_arrayref';
                          }
                        : ( $args[1] = ref($_[2]) ? confess sprintf('reference of type %s not acceptable here', ref $_[2]) : [ @_[2..$#_] ] )
                : defined($_[3]) && !ref($_[3])
                    ? ( $args[2] = $_[3] )
                    : ();
                    
            shift->[DBH]->try_query(@args);
        }],
    ];
    my $r = ref $_[0][DBH];
    for ( @{$switch_on_dbh_class} ) {
        $r =~ $_->[0] and return &{$_->[1]}
    }
    confess "Unknown type of dbh was passed to me: <<$r>>";
}

1;
