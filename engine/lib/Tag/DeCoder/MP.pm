package Tag::DeCoder::MP;
use base 'Data::MessagePack';

sub new {
    my $mp = $_[0]->SUPER::new();
    $mp->canonical->utf8->prefer_integer;
    $mp
}

1;
