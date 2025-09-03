
# Copyright (c) 2021-2025, maintableQL Global Development Group

use strict;
use warnings FATAL => 'all';
use maintableQL::Test::Utils;
use Test::More;

program_help_ok('maintable');
program_version_ok('maintable');
program_options_handling_ok('maintable');

done_testing();
