package Monitoring::Zabipi::Common;
use Exporter qw(import);
use JSON::XS;

our @EXPORT_OK=qw(fillHashInd to_json_str doItemNameExpansion);

sub fillHashInd {
 my ($d,@i)=@_;
 if (@i==1) {
  return \$d->{$i[0]}
 } else {
  my $e=shift @i;
  fillHashInd($d->{$e}=$d->{$e} || {},@i)
 }
}

sub to_json_str {
 my ($cfg,$plStruct)=@_;
 return 0 unless defined $plStruct and ref($plStruct)=~m/^(?:ARRAY|HASH)?$/;
 if (ref $plStruct) {
  return $cfg->{'flPrettyJSON'}?JSON::XS->new->utf8->pretty(1)->encode($plStruct):encode_json($plStruct);
 } else {
  return $cfg->{'flPrettyJSON'}?JSON::XS->new->utf8->pretty(1)->encode(decode_json($plStruct)):$plStruct;
 }
}

sub doItemNameExpansion {
 my ($items,@unsetKeys)=@_;
 
 foreach my $item ( @{$items} ) {
  my ($itemName,$itemKey)=@{$item}{('name','key_')};
  my %h=map { $_=>1 } ($itemName=~m/\$([1-9])/g);
  unless ( %h ) {
   $item->{'name_expanded'}=$itemName;
   next;
  }
  for ($itemKey) {
   s%[^\[]+\[\s*%%;
   s%\]\s*$%%;
  }
  
  my @l=map { s/(?:^['"]|['"]$)//g; $_ } ($itemKey=~m/(?:^|,)\s*("[^"]*"|'[^']*'|[^'",]*)\s*(?=(?:,|$))/g);
  $itemName=~s/\$$_/$l[$_-1]/g foreach keys %h;
  $item->{'name_expanded'}=$itemName;
  delete @{$item}{@unsetKeys} if @unsetKeys;
 }
 return 1;
}

1;
