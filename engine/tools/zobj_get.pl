#!/usr/bin/perl
use 5.16.1;
use utf8;
use strict;
use warnings;
use experimental qw(smartmatch);
use JSON::XS;
use FindBin;
use lib (
  $FindBin::RealBin . '/../lib/app', # first priority
  qw</opt/Perl5/libs /usr/local/share/perl5 /usr/local/lib64/perl5>,
  $FindBin::RealBin . '/../lib/cmn', # least priority
);
use RedC;
use Druid::ZTypes qw(%zobjTypes);

$zobjTypes{'by_letter'}{'c'}{'redis_db'} = 4;

binmode $_, ':utf8' for *STDERR, *STDOUT;
given ($0) {
    when (/_get/) {
        my $redc = RedC->new('name' => 'zobj_getter', 'index' => $zobjTypes{'by_letter'}{'s'}{'redis_db'});

        say JSON::XS->new->pretty->encode(
            +{map {
                /^([shgtdc])((?<=c).+|\d+)$/
                    ? (scalar($1 eq 'c' ? substr($_, 1) : $_) => $redc->select($zobjTypes{'by_letter'}{$1}{'redis_db'} // 0)->read($2))
                    : ()
            } @ARGV}
        );
    }
    when (/_set/) {
        my $zoid = shift;
        die "you must specify valid zoid first" unless $zoid and my ($zoltr, $zloid) = $zoid =~ /^([hgst])(\d+)$/;
        (!@ARGV or scalar(@ARGV) & 1) and die 'you must provide key value pairs to fill zobj attributes';
        my $redc = RedC->new(
            'encoder' 	=> 'MP',
            'name' 	=>  $zobjTypes{'by_letter'}{$zoltr}{'type'} . '_setter',
            'index'	 => $zobjTypes{'by_letter'}{$zoltr}{'redis_db'} // die "redis_db for '$zoltr' not defined"
        );
        my $zobj = $redc->read_not_null($zloid)->[0];
        my $json = JSON::XS->new->allow_nonref;
        my %new_vals = @ARGV;
        for (map \$_, values %new_vals) {
            ${$_} = $json->decode(${$_})
        }
        @{$zobj}{keys %new_vals} = values %new_vals;
        $redc->write($zloid => $zobj);
        say $json->pretty->encode($redc->read_not_null($zloid)->[0]);
    }
}
