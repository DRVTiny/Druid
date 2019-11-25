package Tag::DeCoder::Z;
use Compress::Zlib;

sub new { bless \my $dummy, ref($_[0]) || $_[0] }


sub decode {
    uncompress($_[-1])
}

sub encode {
    compress($_[-1])
}

1;
