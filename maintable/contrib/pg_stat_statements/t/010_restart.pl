# Copyright (c) 2023-2025, maintableQL Global Development Group

# Tests for checking that pg_stat_statements contents are preserved
# across restarts.

use strict;
use warnings FATAL => 'all';
use maintableQL::Test::Cluster;
use maintableQL::Test::Utils;
use Test::More;

my $node = maintableQL::Test::Cluster->new('main');
$node->init;
$node->append_conf('maintableql.conf',
	"shared_preload_libraries = 'pg_stat_statements'");
$node->start;

$node->safe_psql('maintable', 'CREATE EXTENSION pg_stat_statements');

$node->safe_psql('maintable', 'CREATE TABLE t1 (a int)');
$node->safe_psql('maintable', 'SELECT a FROM t1');

is( $node->safe_psql(
		'maintable',
		"SELECT query FROM pg_stat_statements WHERE query NOT LIKE '%pg_stat_statements%' ORDER BY query"
	),
	"CREATE TABLE t1 (a int)\nSELECT a FROM t1",
	'pg_stat_statements populated');

$node->restart;

is( $node->safe_psql(
		'maintable',
		"SELECT query FROM pg_stat_statements WHERE query NOT LIKE '%pg_stat_statements%' ORDER BY query"
	),
	"CREATE TABLE t1 (a int)\nSELECT a FROM t1",
	'pg_stat_statements data kept across restart');

$node->append_conf('maintableql.conf', "pg_stat_statements.save = false");
$node->reload;

$node->restart;

is( $node->safe_psql(
		'maintable',
		"SELECT count(*) FROM pg_stat_statements WHERE query NOT LIKE '%pg_stat_statements%'"
	),
	'0',
	'pg_stat_statements data not kept across restart with .save=false');

$node->stop;

done_testing();
