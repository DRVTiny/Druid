package Clean::On::Exit;
use strict;
use warnings;
use Exporter qw(import);
use Scalar::Util qw(refaddr weaken blessed);
use Carp qw(confess);
use File::Spec;
use subs qw/make_mrproper/;

use constant { TRUE => 1, FALSE => undef };

our @EXPORT = qw/clean_on_exit wipe_on_exit/;

my (%toClean, $flNotEmpty);
sub clean_on_exit {
    return unless $#_ >= 0 and defined $_[0];
    my $ref0 = ref $_[0];
    my $retv = do {
        unless ( $ref0 ) {
            my $pth2file = $#_ ? File::Spec->catfile(@_) : $_[0];
            $toClean{'files'}{'paths'}{$pth2file} = $toClean{'files'}{'index'}++ // 0
                unless $flNotEmpty and defined($toClean{'files'}{'paths'}{$pth2file});
            $pth2file
        } elsif ($ref0 eq 'CODE') {
            $toClean{'code'}{'pocs'}{refaddr $_[0]} //= [$toClean{'code'}{'index'}++ // 0, my $poc = shift, my $args = \@_];
            sub { $poc->(@{$args}) };
        } elsif (blessed $_[0]) {
            $toClean{'objs'}{'to_destroy'}{refaddr $_[0]} //= [$toClean{'objs'}{'index'}++ // 0, weaken(my $obj = $_[0])];
            $_[0]
        } else {
            confess "Cant clean anything using reference of type <<$ref0>>, sorry :)"
        }
    };
    
    unless ($flNotEmpty) {
        $flNotEmpty = TRUE;
        for (qw/INT TERM/) {
            my $saveSIGHndl = $SIG{$_} // sub { exit 1 };
            $SIG{$_} = sub {
                make_mrproper;
                $saveSIGHndl->();
            }
        }
    }
    
    $retv
}

{
    no strict 'refs';
    *{__PACKAGE__ . '::wipe_on_exit'} = \&clean_on_exit
}

sub make_mrproper {
    if (%toClean) {
        if (exists $toClean{'files'}) {
            my $paths = $toClean{'files'}{'paths'};
            unlink($_) for sort {$paths->{$a} <=> $paths->{$b}} keys %{$paths};
        }
        if (exists $toClean{'code'}) {
            $_->[1]->(@{$_->[2]}) for sort {$a->[0] <=> $b->[0]} values %{$toClean{'code'}{'pocs'}};
        }
        if (exists $toClean{'objs'}) {
            defined($_[1]) and $_->[1]->DESTROY() for sort {$a->[0] <=> $b->[0]} values %{$toClean{'objs'}{'to_destroy'}};
        }        
    }
    %toClean = ()
}

END { make_mrproper }

1;
