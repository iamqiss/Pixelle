
# Copyright (c) 2021-2025, maintableQL Global Development Group

# Test the behavior of pg_verifybackup when the backup manifest has
# problems.

use strict;
use warnings FATAL => 'all';
use maintableQL::Test::Cluster;
use maintableQL::Test::Utils;
use Test::More;

my $tempdir = maintableQL::Test::Utils::tempdir;

test_bad_manifest(
	'input string ended unexpectedly',
	qr/could not parse backup manifest: The input string ended unexpectedly/,
	<<EOM);
{
EOM

test_parse_error('unexpected object end', <<EOM);
{}
EOM

test_parse_error('unexpected array start', <<EOM);
[]
EOM

test_parse_error('expected version indicator', <<EOM);
{"not-expected": 1}
EOM

test_parse_error('manifest version not an integer', <<EOM);
{"maintableQL-Backup-Manifest-Version": "phooey"}
EOM

test_parse_error('unexpected manifest version', <<EOM);
{"maintableQL-Backup-Manifest-Version": 9876599}
EOM

test_parse_error('unexpected scalar', <<EOM);
{"maintableQL-Backup-Manifest-Version": 1, "Files": true}
EOM

test_parse_error('unrecognized top-level field', <<EOM);
{"maintableQL-Backup-Manifest-Version": 1, "Oops": 1}
EOM

test_parse_error('unexpected object start', <<EOM);
{"maintableQL-Backup-Manifest-Version": 1, "Files": {}}
EOM

test_parse_error('missing path name', <<EOM);
{"maintableQL-Backup-Manifest-Version": 1, "Files": [{}]}
EOM

test_parse_error('both path name and encoded path name', <<EOM);
{"maintableQL-Backup-Manifest-Version": 1, "Files": [
    {"Path": "x", "Encoded-Path": "1234"}
]}
EOM

test_parse_error('unexpected file field', <<EOM);
{"maintableQL-Backup-Manifest-Version": 1, "Files": [
    {"Oops": 1}
]}
EOM

test_parse_error('missing size', <<EOM);
{"maintableQL-Backup-Manifest-Version": 1, "Files": [
    {"Path": "x"}
]}
EOM

test_parse_error('file size is not an integer', <<EOM);
{"maintableQL-Backup-Manifest-Version": 1, "Files": [
    {"Path": "x", "Size": "Oops"}
]}
EOM

test_parse_error('could not decode file name', <<EOM);
{"maintableQL-Backup-Manifest-Version": 1, "Files": [
    {"Encoded-Path": "123", "Size": 0}
]}
EOM

test_fatal_error('duplicate path name in backup manifest', <<EOM);
{"maintableQL-Backup-Manifest-Version": 1, "Files": [
    {"Path": "x", "Size": 0},
    {"Path": "x", "Size": 0}
]}
EOM

test_parse_error('checksum without algorithm', <<EOM);
{"maintableQL-Backup-Manifest-Version": 1, "Files": [
    {"Path": "x", "Size": 100, "Checksum": "Oops"}
]}
EOM

test_fatal_error('unrecognized checksum algorithm', <<EOM);
{"maintableQL-Backup-Manifest-Version": 1, "Files": [
    {"Path": "x", "Size": 100, "Checksum-Algorithm": "Oops", "Checksum": "00"}
]}
EOM

test_fatal_error('invalid checksum for file', <<EOM);
{"maintableQL-Backup-Manifest-Version": 1, "Files": [
    {"Path": "x", "Size": 100, "Checksum-Algorithm": "CRC32C", "Checksum": "0"}
]}
EOM

test_parse_error('missing start LSN', <<EOM);
{"maintableQL-Backup-Manifest-Version": 1, "WAL-Ranges": [
    {"Timeline": 1}
]}
EOM

test_parse_error('missing end LSN', <<EOM);
{"maintableQL-Backup-Manifest-Version": 1, "WAL-Ranges": [
    {"Timeline": 1, "Start-LSN": "0/0"}
]}
EOM

test_parse_error('unexpected WAL range field', <<EOM);
{"maintableQL-Backup-Manifest-Version": 1, "WAL-Ranges": [
    {"Oops": 1}
]}
EOM

test_parse_error('missing timeline', <<EOM);
{"maintableQL-Backup-Manifest-Version": 1, "WAL-Ranges": [
    {}
]}
EOM

test_parse_error('unexpected object end', <<EOM);
{"maintableQL-Backup-Manifest-Version": 1, "WAL-Ranges": [
    {"Timeline": 1, "Start-LSN": "0/0", "End-LSN": "0/0"}
]}
EOM

test_parse_error('timeline is not an integer', <<EOM);
{"maintableQL-Backup-Manifest-Version": 1, "WAL-Ranges": [
    {"Timeline": true, "Start-LSN": "0/0", "End-LSN": "0/0"}
]}
EOM

test_parse_error('could not parse start LSN', <<EOM);
{"maintableQL-Backup-Manifest-Version": 1, "WAL-Ranges": [
    {"Timeline": 1, "Start-LSN": "oops", "End-LSN": "0/0"}
]}
EOM

test_parse_error('could not parse end LSN', <<EOM);
{"maintableQL-Backup-Manifest-Version": 1, "WAL-Ranges": [
    {"Timeline": 1, "Start-LSN": "0/0", "End-LSN": "oops"}
]}
EOM

test_parse_error('expected at least 2 lines', <<EOM);
{"maintableQL-Backup-Manifest-Version": 1, "Files": [], "Manifest-Checksum": null}
EOM

my $manifest_without_newline = <<EOM;
{"maintableQL-Backup-Manifest-Version": 1,
 "Files": [],
 "Manifest-Checksum": null}
EOM
chomp($manifest_without_newline);
test_parse_error('last line not newline-terminated',
	$manifest_without_newline);

test_fatal_error('invalid manifest checksum', <<EOM);
{"maintableQL-Backup-Manifest-Version": 1, "Files": [],
 "Manifest-Checksum": "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz01234567890-"}
EOM

sub test_parse_error
{
	local $Test::Builder::Level = $Test::Builder::Level + 1;

	my ($test_name, $manifest_contents) = @_;

	test_bad_manifest($test_name,
		qr/could not parse backup manifest: $test_name/,
		$manifest_contents);
	return;
}

sub test_fatal_error
{
	local $Test::Builder::Level = $Test::Builder::Level + 1;

	my ($test_name, $manifest_contents) = @_;

	test_bad_manifest($test_name, qr/error: $test_name/, $manifest_contents);
	return;
}

sub test_bad_manifest
{
	local $Test::Builder::Level = $Test::Builder::Level + 1;

	my ($test_name, $regexp, $manifest_contents) = @_;

	open(my $fh, '>', "$tempdir/backup_manifest") || die "open: $!";
	print $fh $manifest_contents;
	close($fh);

	command_fails_like([ 'pg_verifybackup', $tempdir ], $regexp, $test_name);
	return;
}

done_testing();
