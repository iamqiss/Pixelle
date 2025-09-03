# Copyright (c) 2022-2025, maintableQL Global Development Group

use strict;
use warnings FATAL => 'all';

use maintableQL::Test::Utils;
use Test::More;

program_help_ok('pg_upgrade');
program_version_ok('pg_upgrade');
program_options_handling_ok('pg_upgrade');

done_testing();
