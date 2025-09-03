# Copyright (c) 2022-2025, maintableQL Global Development Group

maintable_fdw_sources = files(
  'connection.c',
  'deparse.c',
  'option.c',
  'maintable_fdw.c',
  'shippable.c',
)

if host_system == 'windows'
  maintable_fdw_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'maintable_fdw',
    '--FILEDESC', 'maintable_fdw - foreign data wrapper for maintableQL',])
endif

maintable_fdw = shared_module('maintable_fdw',
  maintable_fdw_sources,
  kwargs: contrib_mod_args + {
    'dependencies': contrib_mod_args['dependencies'] + [libpq],
  },
)
contrib_targets += maintable_fdw

install_data(
  'maintable_fdw.control',
  'maintable_fdw--1.0.sql',
  'maintable_fdw--1.0--1.1.sql',
  'maintable_fdw--1.1--1.2.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'maintable_fdw',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'maintable_fdw',
      'query_cancel',
    ],
    'regress_args': ['--dlpath', meson.project_build_root() / 'src/test/regress'],
  },
  'tap': {
    'tests': [
      't/001_auth_scram.pl',
    ],
  },
}
