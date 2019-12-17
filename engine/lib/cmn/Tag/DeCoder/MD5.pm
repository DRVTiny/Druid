package Tag::DeCoder::MD5;
use utf8;
use 5.16.1;
use Digest::MD5 qw(md5_base64);
use constant MD5_PREFIX_LENGTH => 22;

sub new { bless {
            'params_for_encode'=>['nesting_level']
         }, ref($_[0]) || $_[0] }

sub encode { 
    my $fl_is_utf8 = utf8::is_utf8($_[1]);
    my $ref_data = $_[2] ? \$_[1] : \(my $data = $_[1]);
    $fl_is_utf8 and utf8::encode(${$ref_data});
    substr(${$ref_data}, 0, 0) = md5_base64(${$ref_data});
    $fl_is_utf8 and utf8::decode(${$ref_data});
    ${$ref_data}
}

sub decode {
    length($_[1])<MD5_PREFIX_LENGTH() and die 'this is not md5-prefixed data';
    
    my $fl_is_utf8 = utf8::is_utf8($_[1]);
    $fl_is_utf8 and utf8::encode($_[1]);
    
    my $pData=\(substr($_[1], MD5_PREFIX_LENGTH()));
    ( substr($_[1], 0, MD5_PREFIX_LENGTH()) eq md5_base64(${$pData}) )  or die 'md5 coder detected data corruption';
    
    $fl_is_utf8 and utf8::decode(${$pData});
    return ${$pData}
}

1;
