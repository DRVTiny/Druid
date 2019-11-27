package ZApi::Controller::Zapiwrap;
use constant DFLT_ZAPI_CONFIG => '/etc/zabbix/api/setenv.conf';
use utf8;
use Mojo::Base 'Mojolicious::Controller';
use JSON::XS qw(encode_json decode_json);
use IO::Socket::SSL;
use Data::Dumper;
use Config::ShellStyle;
my ($zenv, $conf);
BEGIN { $zenv = read_config($conf = $ENV{'ZAPI_CONFIG'} // DFLT_ZAPI_CONFIG) }
my ($apiUrl, $login, $pass, $authToken);

my $flZAPIInit;
sub init {
  my $self = shift;
  my $ua = $self->ua;
  my $log = $self->app->log;
  
  $log->debug('I am pid=' . $$ . '. Initializing connection to Zabbix API...');
  
  $apiUrl = $zenv->{'ZBX_URL'} // die "You must provide value for option <<ZBX_URL>> in your config $conf";
  $apiUrl =~ /https:\/\//i and do {
    require IO::Socket::SSL;
    IO::Socket::SSL::set_defaults(
      SSL_verify_mode => IO::Socket::SSL::SSL_VERIFY_NONE,
    )
  };
  $authToken = $zenv->{'ZBX_TOKEN'} // do {
    ($login, $pass) =
      map $zenv->{$_} // die("You must provide value for option <<$_>> in your config $conf"), qw/ZBX_LOGIN ZBX_PASS/;
    eval { $ua->post(
      $apiUrl,
      { 'Content-Type' => 'application/json' },
      'json' => {'method' => 'user.login', 'jsonrpc' => '2.0', 'id' => 0, 'params' => {'user' => $login, 'password' => $pass}}
    )->result->json->{'result'} } || die "error $@";
  } // do {
    $log->error("Cant initialize API, check connecton parameters in your config $conf");
    return
  };
  
  $log->debug('Connection to Zabbix API established succesfully');
  $flZAPIInit = 1;
}

sub get_tigger_descr {
  my $self = shift;
  my $log = $self->app->log;
  $log->debug( 'Requested triggers: ' . (my $triggerids = $self->param('triggerids') || $self->stash('triggerids') || '') );
  unless ( $flZAPIInit ) {
    $log->error('ZApi not initialised yet, so we have to emergency (re)bootstrap now');
    unless ( init() ) {
      $self->res->code(500);
      $self->render({'error' => 'Zabbix API init() failed. Cant process your request'});
    }
  }
  my @triggers = split /,/ => $triggerids
    or $self->render('json' => {'error' => 'There were no triggerids in request parameters'}), return;

  $self->render_later;
  $self->ua->post(
    $apiUrl,
    {'Content-Type' => 'application/json'},
    'json' => {
      'method' => 'trigger.get', 'jsonrpc' => '2.0', 'id' => 1, 'auth' => $authToken,
      'params' => {'triggerids' => \@triggers, 'expandDescription' => 1,'output' => ['description']}
    },
    sub {
        my ($slfUA, $resp) = @_;
        $self->res->headers->access_control_allow_origin('*');
        unless (my $ans = eval { $resp->result->json }) {
          $self->res->code(501);
          $self->render('json' => {'error' => 'Cant decode answer to Zabbix API request as JSON'});
        } elsif ( $ans->{'error'} ) {
          $self->res->code(404);
          $self->render('json' => {'error' => qq(Zabbix API error $ans->{'error'})});
        } else {
          $self->render('json' => +{map {$_->{'triggerid'} => $_->{'description'}} @{$ans->{'result'}}}, 'gzip' => 1);
        }
    });
}

1;
