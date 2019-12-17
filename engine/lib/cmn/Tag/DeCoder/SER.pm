package Tag::DeCoder::SER;
use constant {
    ENCODER => 0,
    DECODER => 1,
};
use Sereal qw(sereal_encode_with_object
              sereal_decode_with_object);
use Sereal::Encoder;
use Sereal::Decoder;

sub new {
    my ($class, %options) = @_;
    bless [
        Sereal::Encoder->new($options{'encoder_options'} ? %{$options{'encoder_options'}} : ()),
        Sereal::Decoder->new
    ], ref($class) || $class;
}

sub encode {
    sereal_encode_with_object($_[0][ENCODER], $_[1])
}

sub decode {
    sereal_decode_with_object($_[0][DECODER], $_[1])
}

1;
