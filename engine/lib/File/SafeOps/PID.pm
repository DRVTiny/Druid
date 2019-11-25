package File::SafeOps::PID;
use 5.16.1;
use strict;
use warnings;
use utf8;

use File::SafeOps::Handle;
use Try::Tiny;
use Carp qw(confess);

our (@ISA,@EXPORT,@EXPORT_OK);
BEGIN {
	require Exporter;
	@ISA = qw(Exporter);
	@EXPORT = qw(createPIDFile);
        @EXPORT_OK = @EXPORT;
}

sub createPIDFile {
    my ($pidFile, $pidNum, $err)=(shift, shift, undef);
    my $fh = try {
        File::SafeOps::Handle->new(
            $pidFile => 'write',
            'lock_mode'	 => 'exclusive nonblocking',
            'autoremove' => 1,
            'autoflush'  => 1,
        )
    } catch {
        chomp; $err=$_; undef
    } or confess('Cant open PID file for write: ', ($err || 'locked by another process'));
    $fh->write($pidNum // $$);
    return $fh
}

1;
