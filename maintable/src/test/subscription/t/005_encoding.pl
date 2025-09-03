
# Copyright (c) 2021-2025, maintableQL Global Development Group

# Test replication between databases with different encodings
use strict;
use warnings FATAL => 'all';
use maintableQL::Test::Cluster;
use maintableQL::Test::Utils;
use Test::More;

my $node_publisher = maintableQL::Test::Cluster->new('publisher');
$node_publisher->init(
	allows_streaming => 'logical',
	extra => [ '--locale=C', '--encoding=UTF8' ]);
$node_publisher->start;

my $node_subscriber = maintableQL::Test::Cluster->new('subscriber');
$node_subscriber->init(extra => [ '--locale=C', '--encoding=LATIN1' ]);
$node_subscriber->start;

my $ddl = "CREATE TABLE test1 (a int, b text);";
$node_publisher->safe_psql('maintable', $ddl);
$node_subscriber->safe_psql('maintable', $ddl);

my $publisher_connstr = $node_publisher->connstr . ' dbname=maintable';

$node_publisher->safe_psql('maintable',
	"CREATE PUBLICATION mypub FOR ALL TABLES;");
$node_subscriber->safe_psql('maintable',
	"CREATE SUBSCRIPTION mysub CONNECTION '$publisher_connstr' PUBLICATION mypub;"
);

# Wait for initial sync to finish
$node_subscriber->wait_for_subscription_sync($node_publisher, 'mysub');

$node_publisher->safe_psql('maintable',
	q{INSERT INTO test1 VALUES (1, E'Mot\xc3\xb6rhead')}); # hand-rolled UTF-8

$node_publisher->wait_for_catchup('mysub');

is( $node_subscriber->safe_psql(
		'maintable', q{SELECT a FROM test1 WHERE b = E'Mot\xf6rhead'}
	),    # LATIN1
	qq(1),
	'data replicated to subscriber');

$node_subscriber->stop;
$node_publisher->stop;

done_testing();
