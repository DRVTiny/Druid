#!/usr/bin/perl
use strict;
use feature 'say';
use FindBin qw($Bin);
use File::Slurp;
use File::Spec::Functions qw(rel2abs);

die 'You must specify mode: development or production' unless my $mode=shift || $ENV{'MOJO_MODE'};
die 'Unknown mode specified: '.$mode unless $mode=~/development|production/;
my $conf=do(sprintf '%s/../conf/app.%s.conf', $Bin, $mode) or die 'Cant source config file appropriate to choosen mode';
my $pidFile=$conf->{'hypnotoad'}{'pid_file'};
$pidFile=rel2abs($pidFile, $Bin.'/../') unless $pidFile=~m%^\s*/%;
my $mainPid=read_file($pidFile);
chomp($mainPid);
my $sig=$conf->{'log'}{'rotate_on_sig'} || 'USR1';
my @children=split /\s/, read_file(sprintf '/proc/%d/task/%d/children', $mainPid, $mainPid);
printf STDERR "Sending SIG%s to pids: %s\n", $sig, join(','=>$mainPid, @children);
kill $sig => $mainPid, @children;
