#!/usr/bin/perl
use 5.16.1;
use strict;
use warnings;
use Tag::DeCoder;
use Redis::Fast;
use Benchmark qw(timeit timestr :hireswallclock);
my %triggers;
my $estTime = timeit(1 => sub {
    my $r = Redis::Fast->new;
    $r->select(5);
    my @triggerids = $r->keys('*');
    my $c = 0;
    %triggers = map { $triggerids[$c++] => decodeByTag($_) } $r->mget( @triggerids )
});

say 'Estimated time: ' . timestr( $estTime );

