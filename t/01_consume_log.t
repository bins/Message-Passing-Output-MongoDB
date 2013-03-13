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
    connection_options => {
        host => 'localhost:27017',
    },
    database => "log_stash_test",
    collection => "logs",
    indexes => [
        [{foo => 1}]
    ],
    retention => 7,
    verbose => 0,
);

$output->consume({foo => "bar", epochtime => time});

$output->_flush;

my $connection = MongoDB::Connection->new(host => 'localhost', port => 27017);
my $database   = $connection->get_database('log_stash_test');
my $collection_name = 'logs_'. DateTime->now->strftime('%Y%m%d');
my $collection = $database->$collection_name;

is $collection->find_one({ foo => "bar" })->{foo}, "bar", "Found inserted log OK";

my @indexes = $collection->get_indexes;

ok $indexes[1]->{key}->{foo}, "Found indexes OK";

$collection->drop;
$database->drop;

done_testing;

