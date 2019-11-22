package DBR;
use 5.16.1;
use utf8;
use strict;
use warnings;
use boolean;
use Ref::Util qw(is_hashref is_arrayref is_coderef);
use Scalar::Util qw(looks_like_number);
use Try::Tiny;
use Carp qw(confess);
use Data::Dumper qw(Dumper);

use constant {
  SQL_DBMS_NAME 	=> 17,
  DFLT_FUN_SEP_ARGS	=> qr/,\s*/,
  DFLT_STH_METHOD	=> ['fetchall_arrayref' => {}],
  DFLT_DB_QUOTE		=> "'",
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
  my $res =
  try {
    $sth->execute(@{$binds // []});
  } catch {
    confess sprintf join("\n\t" => '', 'when trying to execute SQL query: %s', 'using binds: %s'), ${$p_query}, Dumper($binds);
  };
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

sub __get_info {
  my $dbh = shift;
  if ( $dbh->isa('DBI::db') ) {
      $dbh->get_info(@_);
  } else {
      my $method = ( ref($dbh) =~ /SQLEngine/ ? 'get_' : '' ) . 'dbh';
      $dbh->$method->get_info(@_);
  }
}

sub new {
  my ($class, $dbh, $opts) = @_;
  bless 
  +{
    'dbh'	=> $dbh,
    'backend'	=> +( $switchNativeSelect{(ref($dbh) =~ m/(DBIx?::(?:[^:]+))/)[0]} // die 'dbh class ' . ref($dbh) . ' not supported' ),
    'type'	=> lc __get_info( $dbh, SQL_DBMS_NAME ),
    'rx_fun_sep_args' => DFLT_FUN_SEP_ARGS,
    'named_sql'	=> +{ %sqlByDbType },
    is_hashref($opts) ? %{$opts} : ()
  }, (ref($class) || $class)
}

sub add_named_query {
  $_[0]->{'named_sql'}{$_[1]} = $_[2];
}

sub __open_subst {
  my ($subst, $key, $quo) = @_;
  $quo //= DFLT_DB_QUOTE;
  my ($fmt, $ins) = 
  substr($key, 0, 1) eq '=' 
    ? do {
        if ( defined(my $v = $subst->{substr $key, 1}) ) {
          (is_arrayref($v) ? 'IN(%s)' : '=%s', $v)
        } else {
          ('IS %s', 'NULL')
        }
      }
    : ('%s', $subst->{$key} // 'NULL');
  sprintf($fmt => is_arrayref($ins) ? join(',' => map looks_like_number($_) ? $_ : $quo . $_ . $quo, @{$ins}) : $ins)
}

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
      1 while $query =~ s%\{\{([^}(]+)(?:\(((?:[^{]|\{(?!\{))*?)\))?\}\}%defined($2) ? do { my ($f, $ar) = ($1, $2); $self->__dbr_func($f =~ s/^__dbr_//r, split /$self->{'rx_fun_sep_args'}/, $ar) } : (__open_subst($subst, $1))%ge;
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
  1 while $query =~ s%\{\{([^}(]+)(?:\((.*?)\))?\}\}%defined($2) ? $self->__dbr_func($1 =~ s/^__dbr_//r, split /$self->{'rx_fun_sep_args'}/, $2) : $subst->{$1}%e;
  say 'DO QUERY: <<'.$query.'>>';
  $self->{'dbh'}->do($query, is_arrayref($binds) ? ({}, @{$binds}) : () ) or die $self->{'dbh'}->errstr;
}

sub __dbr_func {
  state $sql4 = +{
    'postgresql' => {
      'instr' 		=> sub { sprintf(q<POSITION(%s IN %s)>, $_[1], $_[0]) },
      'from_unixtime' 	=> q<SUBSTRING(CAST(TO_TIMESTAMP(%s) AS TEXT), '^[^+]+')>,
      'ternary' 	=> q<CASE WHEN (%s) THEN %s ELSE %s END>,
      'iif'		=> '@ternary',
      'list_agg'	=> sub { sprintf(q<STRING_AGG(CAST(%s AS text), %s)>, $_[0], $_[1] || q<','>) },
      'fake_agg'	=> 'MAX(%s)',
      'now_unixts'      => 'EXTRACT(EPOCH FROM NOW())',
      'regexp'		=> '%s ~ %s',
      'iregexp'         => '%s ~* %s',
    },
    'mysql' 	 => {
      'instr' 		=> q<INSTR(%s, %s)>,
      'from_unixtime'   => q<FROM_UNIXTIME(%s)>,
      'ternary'         => q<IF(%s, %s, %s)>,
      'iif'             => '@ternary',
      'list_agg' 	=> sub { sprintf(q<GROUP_CONCAT(%s SEPARATOR %s)>, $_[0], $_[1] || q<','>) },
      'fake_agg'	=> '%s',
      'now_unixts'      => 'UNIX_TIMESTAMP(NOW())',
      'regexp'		=> sub { sprintf 'BINARY %s REGEXP %s', $_[0], $_[1] =~ s%\\%\\\\%gr },
    }
  };
  my ($self, $func) = (shift, shift);
  my $sql4tgt = $sql4->{$self->{'type'}};
  my $queryHndl = $sql4tgt->{$func} // confess sprintf('DBR function %s not implemented for database engine type %s', $func, $self->{'type'});
  
  # dbr function aliases, like "@ternary" for __dbr_iif
  $queryHndl = $sql4tgt->{substr $queryHndl, 1} // die 'illegal dbr function name used'
    while !ref($queryHndl) && substr($queryHndl, 0, 1) eq '@';
    
  is_coderef($queryHndl) 
    ? &{$queryHndl}
    : do { unshift @_, $queryHndl; &{CORE::sprintf} }
}

sub list_columns {
  my ($self, $tablePath) = @_;
  my ($tableName, $tableSchema, $tableCatalog) = my @tpComponents = reverse split /\./ => $tablePath;
  if ( @tpComponents > 2 and $self->{'type'} eq 'mysql' and $tableCatalog ne 'def' ) {
    die sprintf 'There is only table_catalog named "def" in MySQL, i cant use provided value <<%s>> to get table columns information', $tpComponents[0];
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
  say $sql;
  $self->{'dbh'}->selectall_arrayref($sql, {Slice => {}}, $tableName, defined($tableSchema) ? $tableSchema : (), $tableCatalog ? $tableCatalog : ());
}

1;
