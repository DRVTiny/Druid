package File::SafeOps::PID;
use 5.16.1;
use strict;
use warnings;
use utf8;
use constant Y => 1;
use base 'File::SafeOps::Handle';
use Try::Tiny;
use Carp qw(confess);

sub new {
    my ($class, $pidFile, $pidNum) = @_;
    my $err;
    my $fh = try {
        $class->SUPER::new(
            $pidFile 	 => 'write',
            'lock_mode'	 => 'exclusive nonblocking',
            'autoremove' => Y,
            'autoflush'  => Y,
        )
    } catch {
        chomp; $err = $_; undef
    } or confess('Cant open PID file for write: ', ($err || 'locked by another process'));
    (! defined($pidNum) or (length($pidNum) and $pidNum =~ /(?!0)\d\d*/ and -d "/proc/${pidNum}"))
        or confess 'Wrong PID specified: ' . ($pidNum // 'NULL');
    $fh->write($pidNum // $$) or confess(sprintf 'Failed to write PID to the specified pid file <<%s>>', $pidFile);
   
    $fh
}

1;
