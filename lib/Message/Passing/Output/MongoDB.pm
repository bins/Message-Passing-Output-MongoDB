package Message::Passing::Output::MongoDB;

# ABSTRACT: Module for Message::Passing to send log to mongodb

use Moose;
use MongoDB;
use AnyEvent;
use Scalar::Util qw/ weaken /;
use MooseX::Types::Moose qw/ ArrayRef HashRef Str Bool Int Num /;
use Moose::Util::TypeConstraints;
use aliased 'DateTime' => 'DT';
use MooseX::Types::ISO8601 qw/ ISO8601DateTimeStr /;
use Data::Dumper;
use Tie::IxHash;
use namespace::autoclean;

our $VERSION = '0.003';
$VERSION = eval $VERSION;

with qw/
    Message::Passing::Role::Output
    Message::Passing::Role::HasUsernameAndPassword
    Message::Passing::Role::HasHostnameAndPort
/;

has '+password' => (
    required => 0,
);

has '+username' => (
    required => 0,
);

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
            host => $self->hostname,
            port => $self->port,
        );

        my $database = $self->database;
        if (defined $self->username) {
            $connection->authenticate($database, $self->username, $self->password)
            or die "MongoDB authentication failure";
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
    my $collection = $self->_db->$collection_name;

    if ($self->_has_indexes) {
        foreach my $index (@{$self->indexes}){
            $collection->ensure_index(@$index);
            warn("ensure index " . Dumper($index)) if $self->verbose;
        }
    }

    return $collection;
}

sub _default_port { 27017 }

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
    my $collection = $self->_collection;
    $collection->insert($data)
        or warn "Insertion failure: " . Dumper($data) . "\n";
    if ($self->verbose) {
        $self->_inc_log_counter;
        warn("Total " . $self->_log_counter . " records inserted in MongoDB\n");
    }
}

has indexes => (
    isa => ArrayRef[ArrayRef[HashRef]],
    is => 'ro',
    predicate => '_has_indexes',
);

has retention => (
    is => 'ro',
    isa => Num,
    default => 60 * 60 * 24 * 7, # A week
    documentation => 'Int, Time to retent log, in seconds, set 0 to always keep log',
);

has collect_fields => (
    isa => 'Bool',
    is => 'ro',
    default => 0,
);

has _observer => (
    is => 'ro',
    lazy => 1,
    builder => '_build_observer'
);

sub _build_observer {
    my $self = shift;
    weaken($self);
    my $time = 60 * 60 * 24; # Every day
    my $retention_date = DT->from_epoch(epoch => time() - $self->retention );
    AnyEvent->timer(
        after => 30,
        interval => $time,
        cb => sub {
            my $result = $self->_collection->remove(
                { date => { '$lt' => to_ISO8601DateTimeStr($retention_date) } } );
            warn("Cleaned old log failure\n") if !$result;
            warn("Cleaned old log \n") if $self->verbose;
            if ($self->collect_fields){
                eval { 
                    my $map = <<"MAP";
function() {
    for (var key in this) { emit(key, null); }
}
MAP

                    my $reduce = <<"REDUCE";
 function(key, stuff) { return null; }
REDUCE

                    my $cmd = Tie::IxHash->new(
                        "mapreduce" => $self->collection,
                        "map"       => $map,
                        "reduce"    => $reduce,
                        "out"       => $self->collection.'_keys'
                    );

                    my $indexing_result = $self->_db->run_command($cmd);
                    warn($indexing_result) if defined $indexing_result;
                };
                warn "Indexing fields failure : ".Dumper($@) if $@;
            }
        }
    );
}

sub BUILD {
    my ($self) = @_;
    $self->_observer
        if $self->retention != 0;
}

1;

=head1 NAME

Message::Passing::Output::MongoDB - message-passing out put to MongoDB

=head1 SYNOPSIS

    message-pass --input STDIN 
      --output MongoDB --output_options '{"hostname": "localhost", "database":"log_database", "collection":"logs"}'
    
    {"foo":"bar"}

=head1 DESCRIPTION

Module for L<Message::Passing>, send output to MongoDB

=head1 METHODS

=over

=item consume

Consumes a message by JSON encoding it save it in MongoDB

=back

=head1 ATTRIBUTES

=over

=item hostname

Required, Str, your mongodb host

=item database

Required, Str, the database to use.

=item collection

Required, Str, the collection to use.

=item port

Num, the mongodb port, default is 27017

=item username

Str, mongodb authentication user

=item password

Str, mongodb authentication password

=item indexes

ArrayRef[ArrayRef[HashRef]], mongodb indexes

    ...
    indexes => [
        [{"foo" => 1, "bar" => -1}, { unique => true }],
        [{"foo" => 1}],
    ]
    ...

=item collect_fields

Bool, default to 0, set to 1 to collect the fields' key and inserted in collection
$self->collection . "_keys", execution at the starting and once per day.

=item retention

Int, time in seconds to conserver logs, set 0 to keep it permanent, default is
a week

=item verbose

Boolean, verbose

=back

=head1 SEE ALSO

L<Message::Passing>

=head1 SPONSORSHIP

This module exists due to the wonderful people at Suretec Systems Ltd.
<http://www.suretecsystems.com/> who sponsored its development for its
VoIP division called SureVoIP <http://www.surevoip.co.uk/> for use with
the SureVoIP API - 
<http://www.surevoip.co.uk/support/wiki/api_documentation>

=head1 AUTHOR, COPYRIGHT AND LICENSE

See L<Message::Passing>.

=cut

