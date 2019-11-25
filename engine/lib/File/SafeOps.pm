use 5.16.1;
use strict;
use warnings;

package File::SafeOps;
use Carp qw(confess croak);
use File::SafeOps::Handle;
use Try::Tiny;

sub new {
  my ($class, $fileName)=@_;
  die {'error'=>'You must pass file name as the first parameter of the constructor method'}
    unless $fileName and !ref $fileName;
  bless {'path'=>$fileName}, (ref($class) || $class);
}

sub safe_open {
  my ($slf, $mode, $lock, %opts)=@_;
  my $fh=
    try {
      File::SafeOps::Handle->new($slf->{'path'}, $mode, $lock ? ('lock_mode'=>$lock) : (), %opts)
    } catch {
      die {'error'=>$_}
    };
  push @{$slf->{'fh'}}, $fh;
  return $fh
}

sub safe_close {
  my ($slf) = @_;
  $_->close_me() for @{$slf->{'fh'}};
}

sub DESTROY {
  my ($slf)=@_;
  $slf->safe_close();
}

package File::SafeOps::Collection;

sub new {
  my $class=shift;
  my $fco=bless [], (ref $class || $class);
  $fco->add( @_ ) if @_;
  return $fco;
}

sub add {
  my ($slf,@FOs)=@_;
  return unless @FOs;
  die {'error'=>'You can only add objects belongs to File::SafeOps class'}
    if grep { ref($_) ne 'File::SafeOps' } @FOs;
  push @{$slf}, @FOs;
}

sub close_all {
  my $slf=shift;
  undef $_ for @{$slf};
}

sub DESTROY {
  my ($slf)=@_;
  $slf->close_all();
}

1;
