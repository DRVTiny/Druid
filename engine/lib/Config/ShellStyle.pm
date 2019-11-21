package Config::ShellStyle;

use strict;
use warnings;

our $VERSION = '0.5';

use Carp qw(confess);
use Exporter qw(import);

our @EXPORT=qw/read_config/;

sub read_config {
  my $pthSetEnvFile=shift || confess 'You must specify path to your config file';
  open (my $fhSetEnv,'<',$pthSetEnvFile) or die sprintf('Cant get environment from file %s: %s', $pthSetEnvFile, $!);
  my %conf=map { chomp; $_=~m/^\s*(?<KEY>[A-Za-z0-9_-]+)\s*=\s*(?:(?<Q>["'])(?<VAL>((?!\g{Q}).)*)\g{Q}|(?<VAL>[^'"[:space:]]+?))\s*$/?($+{'KEY'},$+{'VAL'}):('NOTHING','NOWHERE') } grep { $_ !~ m/^\s*(?:#.*)?$/ } <$fhSetEnv>;
  if (caller(1) and (caller(1))[3]=~/::BEGIN$/ and exists $conf{'PERL_LIBS'} and $conf{'PERL_LIBS'}) {
    my %INCIndex=do { my $c=0; map {$_=>$c++} split(/\;/ => $conf{'PERL_LIBS'}), @INC };
    @INC=sort {$INCIndex{$a} <=> $INCIndex{$b}} keys %INCIndex;
  }
  return \%conf
}

1;

__END__

=pod

=head1 NAME

Config::ShellStyle - simply read "configs" which can be directly sourced/included from shell scripts

=head1 SYNOPSIS
  
  # In your configuration file
  PARAMETER1='HERE WE GO WITH THE "VALUE1"'
  PARAMETER2="HERE WE GO WITH THE \"VALUE2\""
  PARAMETER3=VALUE3
  
  # In your code
  use Config::ShellStyle
  
  my $config = read_config('/your/config/here.conf') 
    or die 'Failed to read configuration file';
  
  # if you specify special variable PERL_LIBS in your config, 
  # than you can read_config inside BEGIN {} section - and 
  # all directories mentioned in PERL_LIBS (separated by semicolon)
  # wiil be automagically pushed to your @INC
  my %config;
  BEGIN {
    # @INC will be extended to include directories from PERL_LIBS
    %config = eval { %{read_config('/your/config/here.conf')} }
  }

=head1 DESCRIPTION

  Config::ShellStyle exports only one function: read_config - 
  to read shell source files containing only variable definitions 
  (such as those you can see in /etc/sysconfig/* if you use RedHat-like Linux distribution).
  
  All comments in shell source file will be ignored (striped)
  