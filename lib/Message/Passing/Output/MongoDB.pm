package Message::Passing::Output::MongoDB;

# ABSTRACT: Module for Message::Passing to send log to mongodb

use Moose;
use MongoDB;
use AnyEvent;
use Scalar::Util qw/ weaken /;
use MooseX::Types::Moose qw/ ArrayRef HashRef Str Bool Int /;
use Moose::Util::TypeConstraints;
use Try::Tiny qw/ try catch /;
use aliased 'DateTime' => 'DT';
use MooseX::Types::ISO8601 qw/ ISO8601DateTimeStr /;
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
    
    if (defined $self->indexes) {
        foreach my $index (@{$self->indexes}){
            my $result = $collection->ensure_index(@$index);
            warn("ensure index " . Dumper($result)) if $self->verbose;
        }
    }

    return $collection;
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
                warn("Cleaned old log failure\n") if !$result;
                warn("Cleaned old log \n") if $self->verbose;
            },
        );
    },
);

1;

=head1 NAME

Message::Passing::Output::MongoDB - MongoDB output

=head1 SYNOPSIS

    message-pass --input STDIN --output MongoDB --output_options '{"host": "localhost", "database":"log_database", "collection":"logs"}'
    {"foo":"bar"}

=head1 DESCRIPTION

Output messages to File

=head1 METHODS

=head2 consume

Consumes a message by JSON encoding it save it in MongoDB

=head1 ATTRIBUTES

=head2 host

Required, Str, your mongodb host

=head2 database

Required, Str, the database to use.

=head2 collection

Required, Str, the collection to use.

=head2 port

Num, the mongodb port, default is 27017

=head2 user

Str, mongodb authentication user

=head2 password

Str, mongodb authentication password

=head2 indexes

ArrayRef[ArrayRef[HashRef]], mongodb indexes

    ...
    indexes => [
        [{"foo" => 1, "bar" => -1}, { unique => true }],
        [{"foo" => 1}],
    ]
    ...

=head2 retention

Int, time in seconds to conserver logs, set 0 to keep it permanent, default is
a week

=head2 verbose

Boolean, verbose

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

