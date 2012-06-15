use strict;
use warnings;
use Test::More 0.88;
use Try::Tiny;
use MongoDB;

BEGIN {
    try {
        MongoDB::Connection->new(host => 'localhost', port => '27017');
    } catch {
        plan skip_all => $_;
    };
    use_ok('Message::Passing::Output::MongoDB');
}

my $output = Message::Passing::Output::MongoDB->new(
    hostname => "localhost",
    database => "log_stash_test",
    collection => "logs",
    indexes => [
        [{foo => 1}]
    ],
    retention => 0,
    verbose => 0,
);

$output->consume({foo => "bar"});

# Wait 1 seconds to wait output consume
sleep 1;
my $connection = MongoDB::Connection->new(host => 'localhost', port => 27017);
my $database   = $connection->get_database('log_stash_test');
my $collection = $database->logs;

is $collection->find_one({ foo => "bar" })->{foo}, "bar", "Found inserted log OK";

my @indexes = $collection->get_indexes;

ok $indexes[1]->{key}->{foo}, "Found indexes OK";

$collection->drop;
$database->drop;

done_testing;

