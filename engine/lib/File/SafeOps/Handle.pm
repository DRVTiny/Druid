package File::SafeOps::Handle;
use feature 'say';
use Carp qw(confess croak);
use Fcntl qw(:flock SEEK_SET SEEK_END);

my %modeNameCnv=(
  'read'	=>	{ 'open'=>'<' ,	'lock'=>LOCK_SH	},
  'append'	=>	{ 'open'=>'>>',	'lock'=>LOCK_EX },
  'write'	=>	{ 'open'=>'+<', 'lock'=>LOCK_EX },
);

my %lockName2Num=(
  'exclusive'	=>	LOCK_EX,
  'shared'	=>	LOCK_SH,
  'nonblock'	=>	LOCK_NB,
);

sub new {
  my ($class, $fileName, $modeOpen, %opts)=@_;
  
  confess 'Unknown open() mode: '.$modeOpen 
    unless my $how2open=$modeNameCnv{$modeOpen=lc $modeOpen}{'open'};
  
  $opts{'lock_mode'}||=$opts{'lock'};
  die 'Cant determine file lock mode'
    unless my $lockMode=
      $opts{'lock_mode'}
        ? do {
           my ($lm,$ln)=(lc $opts{'lock_mode'},0);
           for (grep $_, split /\W/, $lm) {
             confess('No such lock mode: '.$_) unless defined(my $fl=scan_hash(\%lockName2Num,$_));
             $ln|=$fl
           }
           $ln
          }
        : $modeNameCnv{$modeOpen}{'lock'};
  
  open my $fh, ($modeOpen eq 'write' and ! -e $fileName)?$how2open='>':$how2open, $fileName
    or die "Cant open $fileName for $modeOpen: $!";
  flock($fh, $lockMode) or (($lockMode & LOCK_NB) and $! and substr($!,0,8) eq 'Resource')?return:confess("Cant lock $fileName for [$lockMode]: $!");
    
  if ( $modeOpen=~/append|write/ ) {
    if ( $opts{'autoflush'} ) {
      my $hndl=select($fh);
      $|=1;
      select($hndl);
    }
    
    unless ($how2open ne '+<' 
              or
            $modeOpen eq 'append'
              ? seek($fh, 0, SEEK_END)
              : (seek($fh, 0, SEEK_SET) and truncate($fh, 0))
    ) {
      my $err=$!;
      flock($fh, LOCK_UN);
      die "Cant set seek position in file $fileName to correctly $modeOpen to that file".($err?": $err":'')
    }
  }
  
  bless 
    {'path'=>$fileName, 'handle'=>$fh, 'mode'=>$modeOpen, 'how'=>$how2open, 'opts'=>\%opts},
    (ref $class || $class);
}

sub content {
  my $slf=shift;
  return '' unless $slf->{'handle'} and $slf->{'how'} ne '>';
  seek($slf->{'handle'}, 0, SEEK_SET);
  my @res=map { chomp; $_ } readline($slf->{'handle'});
  seek($slf->{'handle'}, 0, SEEK_END);
  return wantarray?@res:join("\n", @res)
}

sub write {
  my ($slf,@content)=@_;
  return unless $slf->{'handle'} and $slf->{'mode'}=~m/(?:append|write|>)/;
  print { $slf->{'handle'} } join("\n" => @content);
}

sub handle {
  my $fh=shift;
  return $fh->{'handle'};
}

sub mode {
  my $fh=shift;
  return $fh->{'mode'};
}

sub close_me {
  undef $_[0];
}

sub scan_hash {
    my $hr=shift;
    return if ref $_[0] or !(defined $_[0] and ref $hr eq 'HASH');
    my $pS=\$_[0];
    return $hr->{$$pS} if exists $hr->{$$pS};
    my $lps=length($$pS);
    for (grep {length != $lps} keys $hr) {
        return $hr->{$_} if 
          length > $lps
            ? substr($_,0,$lps) eq $$pS
            : substr($$pS,0,length) eq $_
    }
    return
}

sub DESTROY {
  my ($slf)=@_;
#  say 'Destroying file handle';
  if (exists($slf->{'opts'}{'unlink_on_close'}) and $slf->{'opts'}{'unlink_on_close'}) {
    unlink($slf->{'path'}) or 
      die sprintf('When destroying %s object: Cant unlink %s before unlock and close associated filehandler', __PACKAGE__, $slf->{'path'});
  }
  unlink $slf->{'path'} if $slf->{'opts'}{'autoremove'};
  flock $slf->{'handle'}, LOCK_UN;
  close $slf->{'handle'}
  
}

1;
