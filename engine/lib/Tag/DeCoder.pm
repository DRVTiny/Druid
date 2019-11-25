#!/usr/bin/perl
package Tag::DeCoder;
use strict;
use utf8;
use Carp qw(confess);
use Exporter qw(import);

our @EXPORT=our @EXPORT_OK=qw(decodeByTag encodeByTag);

my %knownTags;

sub get_coder_by_tag {
    $knownTags{$_[0]}//=eval sprintf('require Tag::DeCoder::%s; Tag::DeCoder::%s->new', $_[0], $_[0]) or die sprintf('Cant attach coder module for tag %s: %s', $_[0], $@)
}

sub decodeByTag {
    confess 'Insufficient amount of arguments' unless @_;
    do { confess '0-arg is null!'; return } unless defined $_[0];
    return $_[0] unless my $tags=($_[0]=~m/^\{([A-Z\d:]+)\}/)[0];
    if (index($tags, ':')>0) {
        my $data=\(substr($_[0], 2+length($tags)));
        for my $tag (split /:/ => $tags) {
            my $coder=get_coder_by_tag($tag);
            defined(eval { $data=\($coder->decode(${$data})) }) or die 'Error when trying to decode: '.$@;
        }
        defined(wantarray) ? return ${$data} : return($_[0]=${$data});
    } else {
        my $coder=get_coder_by_tag($tags);
        return defined(wantarray) ? $coder->decode(substr($_[0], 2+length($tags))) : return($_[0]=$coder->decode(substr($_[0], 2+length($tags))));
    }
}

sub encodeByTag {
    $#_ or die 'Insufficient amount of arguments';
    my @tags=map split(/,/ => $_), @_[-@_..-2];
    if ($#tags) {
        my $data=$_[-1];
        for my $tag (@tags) {
            my $coder=get_coder_by_tag($tag);
            $data=$coder->encode($data)
        }
        return join('', '{', join(':' => reverse @tags), '}', $data)
    } else {
        my $coder=get_coder_by_tag($_[0]);
        return join(''=>'{', $_[0], '}', $coder->encode($_[1]))
    }
}

1;
