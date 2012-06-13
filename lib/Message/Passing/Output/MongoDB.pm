package Message::Passing::Output::MongoDB;
use Moose;
use MongoDB;
use AnyEvent;
use Scalar::Util qw/ weaken /;
use MooseX::Types::Moose qw/ ArrayRef HashRef Str Bool Int /;
use Moose::Util::TypeConstraints;
use Try::Tiny qw/ try catch /;
use aliased 'DateTime' => 'DT';
use MooseX::Types::ISO8601 qw/ ISO8601DateTimeStr /;
use MooseX::Types::DateTime qw/ DateTime /;
use JSON qw/ encode_json /;
use Data::Dumper;
use namespace::autoclean;

our $VERSION = '0.001';
$VERSION = eval $VERSION;

with 'Message::Passing::Role::Output';

has database => (
    isa => Str,
    is => 'ro',
    required => 1,
);

has _db => (
    is => 'ro',
    isa => 'MongoDB::Database',
    lazy => 1,
    default => sub {
        my $self = shift;
        my $connection = MongoDB::Connection->new( 
            host => $self->host,
            port => $self->port,
        );

        my $database = $self->database;
        if (defined $self->user) {
            $connection->authenticate($database, $self->user, $self->password)
            or die "MongoDB authentication failure";
        }
        if (defined $self->indexes) {
            foreach my $indexe (@{$self->indexes}){
               $connection->ensure_index($indexe);
            }
        }

        return $connection->get_database($self->database);
    },
);

has collection => (
    isa => Str,
    is => 'ro',
    required => 1,
);

has _collection => (
    is       => 'ro',
    isa      => 'MongoDB::Collection',
    lazy     => 1,
    builder  => '_build_logs_collection',
);  

sub _build_logs_collection {
    my ($self) = @_;
    my $collection_name = $self->collection;
    return $self->_db->$collection_name;
}

has host => (
    isa => Str,
    is => 'ro',
    required => 1,
);

has password => (
    isa => Str,
    is => 'ro',
);

has user => (
    isa => Str,
    is => 'ro',
);

has port => (
    isa => Int,
    is => 'ro',
    lazy => 1,
    default => 27017,
);

has _log_counter => (
    traits  => ['Counter'],
    is => 'rw',
    isa => Int,
    default => sub {0},
    handles => { _inc_log_counter => 'inc', },
);

has verbose => (
    isa => 'Bool',
    is => 'ro',
    default => sub {
        -t STDIN
    },
);

sub consume {
    my ($self, $data) = @_;
     return unless $data;
    my $date;
    if (my $epochtime = delete($data->{epochtime})) {
        $date = DT->from_epoch(epoch => $epochtime);
        delete($data->{date});
    }
    elsif (my $try_date = delete($data->{date})) {
        if (is_ISO8601DateTimeStr($try_date)) {
            $date = to_DateTime($try_date);
        }
    }
    $date ||= DT->from_epoch(epoch => time());
    my $type = $data->{__CLASS__} || 'unknown';
    my $record = {
        type => $type,
        data => {
            '@timestamp' => to_ISO8601DateTimeStr($date),
            '@tags' => [],
            '@type' => $type,
            '@source_host' => delete($data->{hostname}) || 'none',
            '@message' => exists($data->{message}) ? delete($data->{message}) : encode_json($data),
            '@fields' => $data,
        },
        exists($data->{uuid}) ? ( id => delete($data->{uuid}) ) : (),
    };
    my $collection = $self->_collection;
    $collection->insert($record)
        or warn "Insertion failure: " . Dumper($record) . "\n";
    if ($self->verbose) {
        $self->_inc_log_counter;
        warn("Total " . $self->_log_counter . " records inserted in MongoDB\n");
    }
}

has indexes => (
    isa => ArrayRef[HashRef],
    is => 'ro',
);

has retention => (
    is => 'ro',
    isa => Int,
    lazy => 1,
    default => 60 * 60 * 24 * 7, # A week
    documentation => 'Int, Time to retent log, in seconds, set 0 to always keep log',
);

has _cleaner => (
    is => 'ro',
    default => sub {
        my $self = shift;
        weaken($self);
        return if $self->retention == 0;
        my $time = 60 * 60 * 24; # Every day
        my $retention_date = DT->from_epoch(epoch => time() - $self->retention );
        AnyEvent->timer(
            after => 100,
            interval => $time,
            cb => sub { 
                my $result = $self->_collection->remove(
                    { date => { '$lt' => to_ISO8601DateTimeStr($retention_date) } } );
                warn("Cleaned old log " . Dumper($result) . "\n") if $self->verbose;
            },
        );
    },
);

1;
