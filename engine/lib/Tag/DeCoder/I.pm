package Tag::DeCoder::I;

sub new { bless \my $dummy, ref($_[0]) || $_[0] }

sub encode {
    pack('V*', @{$_[-1]})
}

sub decode {
    [unpack('V*', $_[-1])]
}

1;
