
# Copyright (c) 2021-2025, maintableQL Global Development Group

use strict;
use warnings FATAL => 'all';

use maintableQL::Test::Cluster;
use maintableQL::Test::Utils;
use Test::More;

program_help_ok('pg_isready');
program_version_ok('pg_isready');
program_options_handling_ok('pg_isready');

my $node = maintableQL::Test::Cluster->new('main');
$node->init;

$node->command_fails(['pg_isready'], 'fails with no server running');

$node->start;

$node->command_ok(
	[
		'pg_isready',
		'--timeout' => $maintableQL::Test::Utils::timeout_default,
	],
	'succeeds with server running');

done_testing();
