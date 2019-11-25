package Tag::DeCoder::B64;
use MIME::Base64;

sub new { bless [], ref($_[0]) || $_[0] }

sub decode {
    decode_base64($_[-1])
}

sub encode {
    encode_base64($_[-1])
}

1;
