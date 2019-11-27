package ZApi;
use Mojo::Base 'Mojolicious';
use ZApi::Controller::Zapiwrap;
use IO::Compress::Gzip qw(gzip);
use POSIX qw(strftime);
use Mojo::Log::Clearable;
use AnyEvent;

my %aeh;
# This method will run once at server start
sub startup {
  my $self = shift;
  
  # Get config
  my $conf=$self->plugin('Config'=>{'file'=>'./conf/app.conf'});
  
  # Setup logging
  $self->app->log(Mojo::Log::Clearable->new('path'=>$conf->{'log'}{'path'}, 'level'=>$conf->{'log'}{'level'}));  
  my $logger=$self->app->log;  
  $logger->warn('Application ZApi started again!');
  $logger->format(sub {
    my ($time,$level,@lines)=@_;
    return sprintf(
      "%s | %s | %d | %s | %s\n",
        split(/ /, strftime('%H:%M:%S %Y-%m-%d' => localtime($time))),
        $$, 
        uc($level),
        sprintf($lines[0], @lines[1..$#lines])
    )
  });
  
  $SIG{my $sigLogRotate=$conf->{'log'}{'rotate_on_sig'}}=sub { $logger->clear_handle };
  # Init controller modules
  ZApi::Controller::Zapiwrap::init($self);
  
  # Setup routing ->
  my $router = $self->routes;
  
#  $self->res->headers->access_control_allow_origin('*');
  
  # Normal route to controller
  $router->any('/triggers')->to('controller'=>'Zapiwrap', 'action'=>'get_tigger_descr');
  # <- Setup routing
  Mojo::IOLoop->singleton->next_tick(sub {
    $logger->debug('next_tick(): Process '.$$.' started');
    $aeh{'rotate_logs'}=AnyEvent->signal(
      'signal'	=>	$sigLogRotate,
      'cb'	=>	sub {
        $logger->debug('Received signal that tell me that log was rotated, clearing log handle...');
        $logger->clear_handle;
        $logger->debug('Happy new log file!');
      },
    );
  });
  $self->hook('after_render' => sub {
      my ($c, $output, $format) = @_;
            
      # Check if "gzip => 1" has been set in the stash
      do { $logger->debug('Compression not required'); return 1 }
        unless $c->stash->{'gzip'};

      # Check if user agent accepts GZip compression
      return unless ($c->req->headers->accept_encoding // '') =~ /gzip/i;
      $c->res->headers->append('Vary' => 'Accept-Encoding');

      # Compress content with GZip
      $c->res->headers->content_encoding('gzip');
      gzip $output, \my $compressed;
      $$output = $compressed;
  });
}

1;
