package DBR;
use 5.16.1;
use utf8;
use strict;
use warnings;
use boolean;
use Ref::Util qw(is_hashref is_arrayref is_coderef);
use Scalar::Util::LooksLikeNumber qw(looks_like_number);
use Scalar::Util qw(blessed);
use Carp qw(confess);
use Data::Dumper qw(Dumper);

use constant {
  SQL_DBMS_NAME 	=> 17,
  DFLT_FUN_SEP_ARGS	=> qr/,\s*/,
  DFLT_STH_METHOD	=> ['fetchall_arrayref' => {}],
  DFLT_DB_QUOTE		=> "'",
  DFLT_TABLE_SCHEMA_PG	=> 'public',
};

use enum qw(:SQL_ QUERIES SUBST PARAMS);

my %sqlByDbType = (
  'table_exists' => {
    'postgresql' => <<'EOSQL',
SELECT COUNT(1) count
FROM information_schema.tables
WHERE
  table_name='{{table_name}}'
    AND
  table_schema=current_schema
    AND
  table_catalog=current_catalog
EOSQL
    'mysql'	 => <<'EOSQL',
SELECT COUNT(1) count
FROM information_schema.tables
WHERE
  table_name='{{table_name}}'
    AND
  table_schema=database()
EOSQL
  },
  'create_table' => {
    'postgresql' => <<'EOSQL',
CREATE TABLE IF NOT EXISTS {{table_name}} (
    serviceid		BIGINT NOT NULL REFERENCES {{services_table_name}} (serviceid) ON UPDATE CASCADE ON DELETE CASCADE,
    {{zobjid_name}}	BIGINT NOT NULL,
    PRIMARY KEY(serviceid, {{zobjid_name}})
)
EOSQL
    'mysql' => <<'EOSQL',
CREATE TABLE IF NOT EXISTS {{table_name}} (
    serviceid BIGINT(20) UNSIGNED NOT NULL,
    {{zobjid_name}} BIGINT(20) UNSIGNED NOT NULL,
    PRIMARY KEY (serviceid, {{zobjid_name}}),
    FOREIGN KEY (serviceid) REFERENCES {{services_table_name}} (serviceid) ON DELETE CASCADE
)
EOSQL
  },
  'drop_table' => <<'EOSQL',
DROP TABLE IF EXISTS {{table_name}}
EOSQL
  'create_table_like' => {
    'postgresql' => <<'EOSQL',
CREATE TABLE {{new_table_name}} () 	INHERITS ({{like_existing}})
EOSQL
    'mysql'	 => <<'EOSQL',
CREATE TABLE {{new_table_name}} 	LIKE 	  {{like_existing}}
EOSQL
  },
  
);

sub __prep_and_exec {
  my ($dbh, $method, $p_query, $binds) = @_;
  my ($method_name, @method_args) = @{$method};
#  say ">> ${$p_query} << method_name=$method_name method_args=".Dumper(\@method_args)." binds=" . Dumper($binds);
  my $sth = $dbh->prepare(${$p_query});
  my $res = $sth->execute(@{$binds // []});
  $method_name eq 'do'
    ? $res
    : $sth->$method_name(@method_args);
}

my %switchNativeSelect = (
  'DBIx::Connector' => sub {
    my ($dbh, $method, $query, $binds) = @_;
    $dbh->run(fixup => sub {
      __prep_and_exec($_, $method, \$query, $binds)
    });
  },
  'DBIx::SQLEngine' => sub {
    my ($dbh, $method, $query, $binds) = @_;
    $dbh->try_query($query, $binds, @{$method});
  },
  'DBI::db' => sub {
    my ($dbh, $method, $query, $binds) = @_;
    $dbh->ping or $dbh = $dbh->clone;
    __prep_and_exec($dbh, $method, \$query, $binds);
  },
);

sub __get_ldbh {
  my $dbh = $_[0]{'dbh'};
  $dbh->isa('DBI::db') 
    ? $dbh
    : do {
        my $method = ( ref($dbh) =~ /SQLEngine/ ? 'get_' : '' ) . 'dbh';
        $dbh->$method
      }
}

sub new {
  my ($class, $dbh, $opts) = @_;
  
  my $self = bless 
  +{
    'dbh'	=> $dbh,
    'backend'	=> +( $switchNativeSelect{(ref($dbh) =~ m/(DBIx?::(?:[^:]+))/)[0]} // die 'dbh class ' . ref($dbh) . ' not supported' ),
    'rx_fun_sep_args' => DFLT_FUN_SEP_ARGS,
    'named_sql'	=> +{ %sqlByDbType },
    is_hashref($opts) ? %{$opts} : ()
  }, (ref($class) || $class);
  my $ldbh = $self->__get_ldbh;
  $self->{'type'} = lc $ldbh->get_info(SQL_DBMS_NAME);
  $self->{'quote_fmt'} = join('%s' => (substr $ldbh->quote(''), 0, 1) x 2);
  $self
}

# simple function, ! class method, ! inst method
sub __db_val {
  my $pv = scalar(@_) == 2 ? \$_[1] : \$_;
  defined( ${$pv} )
    ? (looks_like_number(${$pv}) >> 8)
      ? $_[0]
      : sprintf($_[0], ${$pv})
    : 'NULL'
}

sub __open_subst {
  my ($self, $subst, $key) = @_;
  my ($fmt, $ins) = 
  substr($key, 0, 1) eq '=' 
    ? do {
        if ( defined(my $v = $subst->{substr $key, 1}) ) {
          (is_arrayref($v) ? 'IN(%s)' : '=%s', $v)
        } else {
          ('IS %s', undef)
        }
      }
    : ('%s', $subst->{$key});
  my $qfmt = $self->{'quote_fmt'};
  sprintf($fmt => join(',' => map __db_val($qfmt), is_arrayref($ins) ? @{$ins} : ($ins)))
}

sub compile_query {
  my $self = shift;
  my $pQuery = ref($_[0]) ? $_[0] : do { my $copy0 = $_[0]; \$_[0] };
  my $subst = is_hashref($_[1]) ? $_[1] : +{};
  1 while 
    ${$pQuery} =~ 
    s<
      \{\{([^}(]+)(?:\(((?:[^{]|\{(?!\{))*?)\))?\}\}
    >
    [
      defined($2) 
        ? do { 
            my ($f, $ar) = ($1, $2);
            $self->__dbr_func($f =~ s/^__dbr_//r, split /$self->{'rx_fun_sep_args'}/, $ar)
          }
        : ($self->__open_subst($subst, $1));
    ]gex;
  ref($_[0]) ? $_[0] : ${$pQuery}
}

sub add_named_query {
  blessed($_[0]) 
    or confess 'Using add_named_query as class method is deprecated, now it can be used only as object/instance method';
  $_[0]->{'named_sql'}{$_[1]} = $_[2];
}


# Usage example:
# $dbr->run_queries([
## REQUEST ->
#  	<<EOSQL, 
## SUBST ( {{macro}} ) ->
#	{
#      		'list_agg_sep' => "'" . DFLT_LIST_AGG_SEP . "'", 
#		'host_groups_table' => ${$zapi->fixed_table_name('groups')}
#	},
## BINDS ( ? ) ->
#       [$zbxServer, $eventid]
# ])
sub run_queries {
  my $opts = is_hashref($_[$#_]) ? pop(@_) : {};
  my ($self, @queries) = @_;
  return () unless @queries;
  my ($dbh, $logger) = @{$self}{'dbh', 'logger'};
  my $method = $opts->{'method'} // DFLT_STH_METHOD;
  
  my $cntSelects = 0;
  return map {
    my ($subst, $binds) = @{$_}[SQL_SUBST, SQL_PARAMS];
    $subst //= {}; $binds //= [];
    map {
    # ql_method is a query-level-method
      my ($query, $ql_method) = 
        is_hashref($_)
          ? @{$_}{'sql', 'res_type'}
          : ($_, $method);
      is_arrayref($ql_method) or $ql_method = [$ql_method];
      $self->compile_query(\$query, $subst);
      $logger->debug("QUERY ->\n${query}\n<-QUERY") if $logger;
      $cntSelects += (my $flIsSelect = $query =~ m/^\s*(?i:SELECT|DESC|SHOW)/);
      my $res = $self->{'backend'}->($self->{'dbh'}, $flIsSelect ? $ql_method : ['do'], $query, $binds);
      $flIsSelect ? ($res) : (true);
    } @{$_->[SQL_QUERIES]}
  }
    map is_arrayref($_) 
    ? [do {
        my $c = 0;
        map {
          ($c++ & 1)
            ? defined($_)
              ? is_hashref($_)
                ? $_
                : is_arrayref($_)
                  ? +{@{$_}}
                  : confess('incorrect subst type for query')
              : +{}
            : is_arrayref($_)
              ? $_
              : [$_]
        } @{$_}
      }  ]
    : [[$_], {}, []],
      @queries
}

sub exec_named_query {
  my ($self, $queryName) = (shift, shift);
  my $qbyt = $self->{'named_sql'}{$queryName} or die 'no such query name known: ' . $queryName . Dumper($self->{'named_sql'}{$queryName});
  my %pars = @_;
  my $subst = is_hashref( $pars{'subst'} )  ? $pars{'subst'} : +{};
  my $binds = is_arrayref($pars{'binds'} )  ? $pars{'binds'} : +[];
  my $method  = defined( $pars{'method'} // $pars{'method_args'} ) ? [ $pars{'method'} // DFLT_STH_METHOD->[0], @{$pars{'method_args'}} ] : undef;
  my $p_queries = 
    is_hashref($qbyt)
    ? do {
    # If queryName implemented in different ways depending on database engine used
        my $dbType = $self->{'type'};
        exists( $qbyt->{$dbType} ) or die sprintf("database engine type %s not supported for queryName %s\n", $dbType, $queryName);
        \$qbyt->{$dbType}
      }
    : \$qbyt;
  $self->run_queries( 
    [${$p_queries}, $subst, $binds], 
    $method ? {'method' => $method} : ()
  );
}

{
  no strict 'refs';
  *{__PACKAGE__ . '::named_query' } = \&exec_named_query;
}
sub set_fun_sep_args {
  $_[0]{'rx_fun_sep_args'} = ref($_[1]) eq 'Regexp' ? $_[1] : qr($_[1]);
}

sub do {
  no strict 'refs';
  my ($self, $query, $subst, $binds, $opts) = @_;
  if (is_hashref $opts) {
    for ( map [each %{$opts}], 1..keys %{$opts} ) {
      my $method = 'set_' . lc($_->[0]);
      $self->$method($_->[1]);
    }
  }
  $subst //= {};
  $self->compile_query(\$query, $subst);
  $self->{'dbh'}->do($query, is_arrayref($binds) ? ({}, @{$binds}) : () ) or die $self->{'dbh'}->errstr;
}

sub __dbr_func {
  state $sql4 = +{
    'postgresql' => {
      'instr' 		=> sub { sprintf(q<POSITION(%s IN %s)>, $_[1], $_[0]) },
      'from_unixtime' 	=> q<SUBSTRING(CAST(TO_TIMESTAMP(%s) AS TEXT), '^[^+]+')>,
      'ternary' 	=> q<CASE WHEN (%s) THEN %s ELSE %s END>,
      'list_agg'	=> sub { sprintf(q<STRING_AGG(CAST(%s AS text), %s)>, $_[0], $_[1] || q<','>) },
      'fake_agg'	=> 'MAX(%s)',
      'now_unixts'      => 'EXTRACT(EPOCH FROM NOW())',
#      'unix_timestamp'	=> q<EXTRACT(EPOCH FROM TO_TIMESTAMP(%s, 'YYYY-MM-DD HH24:MI:SS'))>,
      'unix_timestamp' 	=>  q<EXTRACT(EPOCH FROM %s::timestamptz)>,
      'regexp'		=> '%s ~ %s',
      'iregexp'         => '%s ~* %s',
    },
    'mysql' 	 => {
      'instr' 		=> q<INSTR(%s, %s)>,
      'from_unixtime'   => q<FROM_UNIXTIME(%s)>,
      'ternary'         => q<IF(%s, %s, %s)>,
      'list_agg' 	=> sub { sprintf(q<GROUP_CONCAT(%s SEPARATOR %s)>, $_[0], $_[1] || q<','>) },
      'fake_agg'	=> '%s',
      'now_unixts'      => 'UNIX_TIMESTAMP(NOW())',
      'unix_timestamp'  => 'UNIX_TIMESTAMP(%s)',
      'regexp'		=> sub { sprintf 'BINARY %s REGEXP %s', $_[0], $_[1] =~ s%\\%\\\\%gr },
    }
  };
  state $func_aliases = {
    'list_aggr' => 'list_agg',
    'aggr_list' => 'list_agg',
    'iif'	=>  'ternary',
  };
  my ($self, $func) = (shift, shift);
  my $sql4tgt = $sql4->{$self->{'type'}};
  my $queryHndl = $sql4tgt->{$func} // 
                    $sql4tgt->{$func_aliases->{$func}} //
                      confess sprintf('DBR function %s not implemented for database engine type %s', $func, $self->{'type'});  
    
  is_coderef($queryHndl)
    ? &{$queryHndl}
    : do { unshift @_, $queryHndl; &{CORE::sprintf} }
}

sub list_columns {
  my ($self, $tablePath) = @_;
  my ($tableName, $tableSchema, $tableCatalog) = my @tpComponents = reverse split /\./ => $tablePath;
  
  if ( @tpComponents > 2 and $self->{'type'} eq 'mysql' and $tableCatalog ne 'def' ) {
    if (! $tableSchema or $tableSchema eq DFLT_TABLE_SCHEMA_PG) {
      $tableSchema = $tableCatalog;
      undef $tableCatalog;
    } else {
      die sprintf 'There is only table_catalog named "def" in MySQL, i cant use provided value <<%s>> to get table columns information', $tpComponents[0];
    }
  }
  my $sql = sprintf <<EOSQL, defined($tableSchema) ? 'AND table_schema=?' : $self->{'type'} eq 'postgresql' ? do { $tableSchema = 'public'; 'AND table_schema=?' } : '', defined($tableCatalog) ? 'AND table_catalog=?' : '';
  SELECT
    column_name,
    data_type
  FROM
    information_schema.columns
  WHERE
    table_name=?
    %s
    %s
EOSQL
  $self->{'dbh'}->selectall_arrayref($sql, {Slice => {}}, $tableName, defined($tableSchema) ? $tableSchema : (), $tableCatalog ? $tableCatalog : ());
}

1;
