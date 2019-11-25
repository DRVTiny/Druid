package Tag::DeCoder::II;

sub new { bless \my $dummy, ref($_[0]) || $_[0] }

sub decode {
    my @stream=unpack('V*',$_[-1]);
    my ($i,@res);
    do {
        push @res, [ @stream[($i+1)..($i+=$stream[$i])] ]
    } until $i++==$#stream;
    return \@res
}

sub encode {
    pack('V*', map { scalar(@{$_}), @{$_} } @{$_[-1]})
}

1;
