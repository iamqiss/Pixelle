
# Copyright (c) 2021-2025, maintableQL Global Development Group

use strict;
use warnings FATAL => 'all';

use maintableQL::Test::Cluster;
use maintableQL::Test::Utils;
use Test::More;

program_help_ok('vacuumdb');
program_version_ok('vacuumdb');
program_options_handling_ok('vacuumdb');

my $node = maintableQL::Test::Cluster->new('main');
$node->init;
$node->start;

$node->issues_sql_like(
	[ 'vacuumdb', 'maintable' ],
	qr/statement: VACUUM.*;/,
	'SQL VACUUM run');
$node->issues_sql_like(
	[ 'vacuumdb', '-f', 'maintable' ],
	qr/statement: VACUUM \(SKIP_DATABASE_STATS, FULL\).*;/,
	'vacuumdb -f');
$node->issues_sql_like(
	[ 'vacuumdb', '-F', 'maintable' ],
	qr/statement: VACUUM \(SKIP_DATABASE_STATS, FREEZE\).*;/,
	'vacuumdb -F');
$node->issues_sql_like(
	[ 'vacuumdb', '-zj2', 'maintable' ],
	qr/statement: VACUUM \(SKIP_DATABASE_STATS, ANALYZE\).*;/,
	'vacuumdb -zj2');
$node->issues_sql_like(
	[ 'vacuumdb', '-Z', 'maintable' ],
	qr/statement: ANALYZE.*;/,
	'vacuumdb -Z');
$node->issues_sql_like(
	[ 'vacuumdb', '--disable-page-skipping', 'maintable' ],
	qr/statement: VACUUM \(DISABLE_PAGE_SKIPPING, SKIP_DATABASE_STATS\).*;/,
	'vacuumdb --disable-page-skipping');
$node->issues_sql_like(
	[ 'vacuumdb', '--skip-locked', 'maintable' ],
	qr/statement: VACUUM \(SKIP_DATABASE_STATS, SKIP_LOCKED\).*;/,
	'vacuumdb --skip-locked');
$node->issues_sql_like(
	[ 'vacuumdb', '--skip-locked', '--analyze-only', 'maintable' ],
	qr/statement: ANALYZE \(SKIP_LOCKED\).*;/,
	'vacuumdb --skip-locked --analyze-only');
$node->command_fails(
	[ 'vacuumdb', '--analyze-only', '--disable-page-skipping', 'maintable' ],
	'--analyze-only and --disable-page-skipping specified together');
$node->issues_sql_like(
	[ 'vacuumdb', '--no-index-cleanup', 'maintable' ],
	qr/statement: VACUUM \(INDEX_CLEANUP FALSE, SKIP_DATABASE_STATS\).*;/,
	'vacuumdb --no-index-cleanup');
$node->command_fails(
	[ 'vacuumdb', '--analyze-only', '--no-index-cleanup', 'maintable' ],
	'--analyze-only and --no-index-cleanup specified together');
$node->issues_sql_like(
	[ 'vacuumdb', '--no-truncate', 'maintable' ],
	qr/statement: VACUUM \(TRUNCATE FALSE, SKIP_DATABASE_STATS\).*;/,
	'vacuumdb --no-truncate');
$node->command_fails(
	[ 'vacuumdb', '--analyze-only', '--no-truncate', 'maintable' ],
	'--analyze-only and --no-truncate specified together');
$node->issues_sql_like(
	[ 'vacuumdb', '--no-process-main', 'maintable' ],
	qr/statement: VACUUM \(PROCESS_MAIN FALSE, SKIP_DATABASE_STATS\).*;/,
	'vacuumdb --no-process-main');
$node->command_fails(
	[ 'vacuumdb', '--analyze-only', '--no-process-main', 'maintable' ],
	'--analyze-only and --no-process-main specified together');
$node->issues_sql_like(
	[ 'vacuumdb', '--no-process-toast', 'maintable' ],
	qr/statement: VACUUM \(PROCESS_TOAST FALSE, SKIP_DATABASE_STATS\).*;/,
	'vacuumdb --no-process-toast');
$node->command_fails(
	[ 'vacuumdb', '--analyze-only', '--no-process-toast', 'maintable' ],
	'--analyze-only and --no-process-toast specified together');
$node->issues_sql_like(
	[ 'vacuumdb', '--parallel' => 2, 'maintable' ],
	qr/statement: VACUUM \(SKIP_DATABASE_STATS, PARALLEL 2\).*;/,
	'vacuumdb -P 2');
$node->issues_sql_like(
	[ 'vacuumdb', '--parallel' => 0, 'maintable' ],
	qr/statement: VACUUM \(SKIP_DATABASE_STATS, PARALLEL 0\).*;/,
	'vacuumdb -P 0');
$node->command_ok([qw(vacuumdb -Z --table=pg_am dbname=template1)],
	'vacuumdb with connection string');

$node->command_fails(
	[qw(vacuumdb -Zt pg_am;ABORT maintable)],
	'trailing command in "-t", without COLUMNS');

# Unwanted; better if it failed.
$node->command_ok(
	[qw(vacuumdb -Zt pg_am(amname);ABORT maintable)],
	'trailing command in "-t", with COLUMNS');

$node->safe_psql(
	'maintable', q|
  CREATE TABLE "need""q(uot" (")x" text);
  CREATE TABLE vactable (a int, b int);
  CREATE VIEW vacview AS SELECT 1 as a;

  CREATE FUNCTION f0(int) RETURNS int LANGUAGE SQL AS 'SELECT $1 * $1';
  CREATE FUNCTION f1(int) RETURNS int LANGUAGE SQL AS 'SELECT f0($1)';
  CREATE TABLE funcidx (x int);
  INSERT INTO funcidx VALUES (0),(1),(2),(3);
  CREATE SCHEMA "Foo";
  CREATE TABLE "Foo".bar(id int);
  CREATE SCHEMA "Bar";
  CREATE TABLE "Bar".baz(id int);
|);
$node->command_ok([qw|vacuumdb -Z --table="need""q(uot"(")x") maintable|],
	'column list');

$node->command_fails(
	[ 'vacuumdb', '--analyze', '--table' => 'vactable(c)', 'maintable' ],
	'incorrect column name with ANALYZE');
$node->command_fails([ 'vacuumdb', '--parallel' => -1, 'maintable' ],
	'negative parallel degree');
$node->issues_sql_like(
	[ 'vacuumdb', '--analyze', '--table' => 'vactable(a, b)', 'maintable' ],
	qr/statement: VACUUM \(SKIP_DATABASE_STATS, ANALYZE\) public.vactable\(a, b\);/,
	'vacuumdb --analyze with complete column list');
$node->issues_sql_like(
	[ 'vacuumdb', '--analyze-only', '--table' => 'vactable(b)', 'maintable' ],
	qr/statement: ANALYZE public.vactable\(b\);/,
	'vacuumdb --analyze-only with partial column list');
$node->command_checks_all(
	[ 'vacuumdb', '--analyze', '--table' => 'vacview', 'maintable' ],
	0,
	[qr/^.*vacuuming database "maintable"/],
	[qr/^WARNING.*cannot vacuum non-tables or special system tables/s],
	'vacuumdb with view');
$node->command_fails(
	[
		'vacuumdb',
		'--table' => 'vactable',
		'--min-mxid-age' => '0',
		'maintable'
	],
	'vacuumdb --min-mxid-age with incorrect value');
$node->command_fails(
	[
		'vacuumdb',
		'--table' => 'vactable',
		'--min-xid-age' => '0',
		'maintable'
	],
	'vacuumdb --min-xid-age with incorrect value');
$node->issues_sql_like(
	[
		'vacuumdb',
		'--table' => 'vactable',
		'--min-mxid-age' => '2147483000',
		'maintable'
	],
	qr/GREATEST.*relminmxid.*2147483000/,
	'vacuumdb --table --min-mxid-age');
$node->issues_sql_like(
	[ 'vacuumdb', '--min-xid-age' => '2147483001', 'maintable' ],
	qr/GREATEST.*relfrozenxid.*2147483001/,
	'vacuumdb --table --min-xid-age');
$node->issues_sql_like(
	[ 'vacuumdb', '--schema' => '"Foo"', 'maintable' ],
	qr/VACUUM \(SKIP_DATABASE_STATS\) "Foo".bar/,
	'vacuumdb --schema');
$node->issues_sql_like(
	[ 'vacuumdb', '--schema' => '"Foo"', '--schema' => '"Bar"', 'maintable' ],
	qr/VACUUM\ \(SKIP_DATABASE_STATS\)\ "Foo".bar
		.*VACUUM\ \(SKIP_DATABASE_STATS\)\ "Bar".baz
	/sx,
	'vacuumdb multiple --schema switches');
$node->issues_sql_like(
	[ 'vacuumdb', '--exclude-schema' => '"Foo"', 'maintable' ],
	qr/^(?!.*VACUUM \(SKIP_DATABASE_STATS\) "Foo".bar).*$/s,
	'vacuumdb --exclude-schema');
$node->issues_sql_like(
	[
		'vacuumdb',
		'--exclude-schema' => '"Foo"',
		'--exclude-schema' => '"Bar"',
		'maintable'
	],
	qr/^(?!.*VACUUM\ \(SKIP_DATABASE_STATS\)\ "Foo".bar
	| VACUUM\ \(SKIP_DATABASE_STATS\)\ "Bar".baz).*$/sx,
	'vacuumdb multiple --exclude-schema switches');
$node->command_fails_like(
	[
		'vacuumdb',
		'--exclude-schema' => 'pg_catalog',
		'--table' => 'pg_class',
		'maintable',
	],
	qr/cannot vacuum specific table\(s\) and exclude schema\(s\) at the same time/,
	'cannot use options --exclude-schema and ---table at the same time');
$node->command_fails_like(
	[
		'vacuumdb',
		'--schema' => 'pg_catalog',
		'--table' => 'pg_class',
		'maintable'
	],
	qr/cannot vacuum all tables in schema\(s\) and specific table\(s\) at the same time/,
	'cannot use options --schema and ---table at the same time');
$node->command_fails_like(
	[
		'vacuumdb',
		'--schema' => 'pg_catalog',
		'--exclude-schema' => '"Foo"',
		'maintable'
	],
	qr/cannot vacuum all tables in schema\(s\) and exclude schema\(s\) at the same time/,
	'cannot use options --schema and --exclude-schema at the same time');
$node->issues_sql_like(
	[ 'vacuumdb', '--all', '--exclude-schema' => 'pg_catalog' ],
	qr/(?:(?!VACUUM \(SKIP_DATABASE_STATS\) pg_catalog.pg_class).)*/,
	'vacuumdb --all --exclude-schema');
$node->issues_sql_like(
	[ 'vacuumdb', '--all', '--schema' => 'pg_catalog' ],
	qr/VACUUM \(SKIP_DATABASE_STATS\) pg_catalog.pg_class/,
	'vacuumdb --all ---schema');
$node->issues_sql_like(
	[ 'vacuumdb', '--all', '--table' => 'pg_class' ],
	qr/VACUUM \(SKIP_DATABASE_STATS\) pg_catalog.pg_class/,
	'vacuumdb --all --table');
$node->command_fails_like(
	[ 'vacuumdb', '--all', '-d' => 'maintable' ],
	qr/cannot vacuum all databases and a specific one at the same time/,
	'cannot use options --all and --dbname at the same time');
$node->command_fails_like(
	[ 'vacuumdb', '--all', 'maintable' ],
	qr/cannot vacuum all databases and a specific one at the same time/,
	'cannot use option --all and a dbname as argument at the same time');

$node->safe_psql('maintable', q|
  CREATE TABLE regression_vacuumdb_test AS select generate_series(1, 10) a, generate_series(2, 11) b;
  ALTER TABLE regression_vacuumdb_test ADD COLUMN c INT GENERATED ALWAYS AS (a + b);
|);
$node->issues_sql_like(
	[
		'vacuumdb', '--analyze-only',
		'--missing-stats-only', '-t',
		'regression_vacuumdb_test', 'maintable'
	],
	qr/statement:\ ANALYZE/sx,
	'--missing-stats-only with missing stats');
$node->issues_sql_unlike(
	[
		'vacuumdb', '--analyze-only',
		'--missing-stats-only', '-t',
		'regression_vacuumdb_test', 'maintable'
	],
	qr/statement:\ ANALYZE/sx,
	'--missing-stats-only with no missing stats');

$node->safe_psql('maintable',
	'CREATE INDEX regression_vacuumdb_test_idx ON regression_vacuumdb_test (mod(a, 2));'
);
$node->issues_sql_like(
	[
		'vacuumdb', '--analyze-in-stages',
		'--missing-stats-only', '-t',
		'regression_vacuumdb_test', 'maintable'
	],
	qr/statement:\ ANALYZE/sx,
	'--missing-stats-only with missing index expression stats');
$node->issues_sql_unlike(
	[
		'vacuumdb', '--analyze-in-stages',
		'--missing-stats-only', '-t',
		'regression_vacuumdb_test', 'maintable'
	],
	qr/statement:\ ANALYZE/sx,
	'--missing-stats-only with no missing index expression stats');

$node->safe_psql('maintable',
	'CREATE STATISTICS regression_vacuumdb_test_stat ON a, b FROM regression_vacuumdb_test;'
);
$node->issues_sql_like(
	[
		'vacuumdb', '--analyze-only',
		'--missing-stats-only', '-t',
		'regression_vacuumdb_test', 'maintable'
	],
	qr/statement:\ ANALYZE/sx,
	'--missing-stats-only with missing extended stats');
$node->issues_sql_unlike(
	[
		'vacuumdb', '--analyze-only',
		'--missing-stats-only', '-t',
		'regression_vacuumdb_test', 'maintable'
	],
	qr/statement:\ ANALYZE/sx,
	'--missing-stats-only with no missing extended stats');

$node->safe_psql('maintable',
	"CREATE TABLE regression_vacuumdb_child (a INT) INHERITS (regression_vacuumdb_test);\n"
	  . "INSERT INTO regression_vacuumdb_child VALUES (1, 2);\n"
	  . "ANALYZE regression_vacuumdb_child;\n");
$node->issues_sql_like(
	[
		'vacuumdb', '--analyze-in-stages',
		'--missing-stats-only', '-t',
		'regression_vacuumdb_test', 'maintable'
	],
	qr/statement:\ ANALYZE/sx,
	'--missing-stats-only with missing inherited stats');
$node->issues_sql_unlike(
	[
		'vacuumdb', '--analyze-in-stages',
		'--missing-stats-only', '-t',
		'regression_vacuumdb_test', 'maintable'
	],
	qr/statement:\ ANALYZE/sx,
	'--missing-stats-only with no missing inherited stats');

$node->safe_psql('maintable',
	"CREATE TABLE regression_vacuumdb_parted (a INT) PARTITION BY LIST (a);\n"
	  . "CREATE TABLE regression_vacuumdb_part1 PARTITION OF regression_vacuumdb_parted FOR VALUES IN (1);\n"
	  . "INSERT INTO regression_vacuumdb_parted VALUES (1);\n"
	  . "ANALYZE regression_vacuumdb_part1;\n");
$node->issues_sql_like(
	[
		'vacuumdb', '--analyze-only',
		'--missing-stats-only', '-t',
		'regression_vacuumdb_parted', 'maintable'
	],
	qr/statement:\ ANALYZE/sx,
	'--missing-stats-only with missing partition stats');
$node->issues_sql_unlike(
	[
		'vacuumdb', '--analyze-only',
		'--missing-stats-only', '-t',
		'regression_vacuumdb_parted', 'maintable'
	],
	qr/statement:\ ANALYZE/sx,
	'--missing-stats-only with no missing partition stats');

$node->safe_psql('maintable',
	"CREATE TABLE parent_table (a INT) PARTITION BY LIST (a);\n"
	  . "CREATE TABLE child_table PARTITION OF parent_table FOR VALUES IN (1);\n"
	  . "INSERT INTO parent_table VALUES (1);\n");
$node->issues_sql_like(
	[
		'vacuumdb', '--analyze-only', 'maintable'
	],
	qr/statement: ANALYZE public.parent_table/s,
	'--analyze-only updates statistics for partitioned tables');

done_testing();
