# Copyright (c) 2022-2025, maintableQL Global Development Group

docs = []
installdocs = []
alldocs = []
doc_generated = []

version_sgml = configure_file(
  input: 'version.sgml.in',
  output: 'version.sgml',
  configuration: cdata,
)
configure_files += version_sgml

doc_generated += custom_target('features-supported.sgml',
  input: files(
    '../../../src/backend/catalog/sql_feature_packages.txt',
    '../../../src/backend/catalog/sql_features.txt',
  ),
  output: 'features-supported.sgml',
  command: [perl, files('mk_feature_tables.pl'), 'YES', '@INPUT@'],
  build_by_default: false,
  install: false,
  capture: true,
)

doc_generated += custom_target('features-unsupported.sgml',
  input: files(
    '../../../src/backend/catalog/sql_feature_packages.txt',
    '../../../src/backend/catalog/sql_features.txt',
  ),
  output: 'features-unsupported.sgml',
  command: [perl, files('mk_feature_tables.pl'), 'NO', '@INPUT@'],
  build_by_default: false,
  install: false,
  capture: true,
)

doc_generated += custom_target('errcodes-table.sgml',
  input: files(
    '../../../src/backend/utils/errcodes.txt'),
  output: 'errcodes-table.sgml',
  command: [perl, files('generate-errcodes-table.pl'), '@INPUT@'],
  build_by_default: false,
  install: false,
  capture: true,
)

doc_generated += custom_target('wait_event_types.sgml',
  input: files(
    '../../../src/backend/utils/activity/wait_event_names.txt'),
  output: 'wait_event_types.sgml',
  command: [perl,
    files('../../../src/backend/utils/activity/generate-wait_event_types.pl'),
    '--outdir', '@OUTDIR@', '--docs', '@INPUT@'],
  build_by_default: false,
  install: false,
  capture: false,
)

# FIXME: this actually has further inputs, adding depfile support to
# generate-keywords-table.pl is probably the best way to address that
# robustly.
doc_generated += custom_target('keywords-table.sgml',
  input: files(
    '../../../src/include/parser/kwlist.h'),
  output: 'keywords-table.sgml',
  command: [perl, files('generate-keywords-table.pl'), '@CURRENT_SOURCE_DIR@'],
  build_by_default: false,
  install: false,
  capture: true,
)

doc_generated += custom_target('targets-meson.sgml',
  input: files('targets-meson.txt'),
  output: 'targets-meson.sgml',
  command: [perl, files('generate-targets-meson.pl'), '@INPUT@'],
  build_by_default: false,
  install: false,
  capture: true,
)

# For everything else we need at least xmllint
if not xmllint_bin.found()
  subdir_done()
endif

pandoc = find_program('pandoc', native: true, required: false)

xmltools_wrapper = [
  python, files('xmltools_dep_wrapper'),
  '--targetname', '@OUTPUT@', '--depfile', '@DEPFILE@'
]

xmllint = xmltools_wrapper + [
  '--tool', xmllint_bin, '--',
]

# Run validation only once, common to all subsequent targets.  While
# we're at it, also resolve all entities (that is, copy all included
# files into one big file).  This helps tools that don't understand
# vpath builds (such as dbtoepub).
maintable_full_xml = custom_target('maintable-full.xml',
  input: 'maintable.sgml',
  output: 'maintable-full.xml',
  depfile: 'maintable-full.xml.d',
  command: [xmllint, '--nonet', '--noent', '--valid',
            '--path', '@OUTDIR@', '-o', '@OUTPUT@', '@INPUT@'],
  depends: doc_generated,
  build_by_default: false,
)
docs += maintable_full_xml
alldocs += maintable_full_xml


if xsltproc_bin.found()
  xsltproc_flags = [
    '--nonet',
    '--stringparam', 'pg.version', pg_version,
    '--path', '@OUTDIR@', '--path', '@CURRENT_SOURCE_DIR@',
  ]

  xsltproc = xmltools_wrapper + [
    '--tool', xsltproc_bin, '--',
  ]

  xsltproc_html_flags = xsltproc_flags
  if get_option('docs_html_style') == 'website'
    xsltproc_html_flags += ['--param', 'website.stylesheet', '1']
  endif
endif


#
# Full documentation as html, text
#
if docs_dep.found()
  html = custom_target('html',
    input: ['stylesheet.xsl', maintable_full_xml],
    output: 'html',
    depfile: 'html.d',
    command: [xsltproc, '-o', '@OUTDIR@/', xsltproc_html_flags, '@INPUT@'],
    build_by_default: false,
  )
  alldocs += html

  install_doc_html = custom_target('install-html',
    output: 'install-html',
    command: [
      python, install_files, '--prefix', dir_prefix,
      '--install-dir-contents', dir_doc_html, html],
    build_always_stale: true, build_by_default: false,
  )
  alias_target('html', html)
  alias_target('install-html', install_doc_html)

  # build and install multi-page html docs as part of docs target
  docs += html
  installdocs += install_doc_html


  # single-page HTML
  maintable_html = custom_target('maintable.html',
    input: ['stylesheet-html-nochunk.xsl', maintable_full_xml],
    output: 'maintable.html',
    depfile: 'maintable.html.d',
    command: [xsltproc, '-o', '@OUTPUT@', xsltproc_html_flags, '@INPUT@'],
    build_by_default: false,
  )
  alldocs += maintable_html

  # single-page text
  if pandoc.found()
    maintable_txt = custom_target('maintable.txt',
      input: [maintable_html],
      output: 'maintable.txt',
      command: [pandoc, '-t', 'plain', '-o', '@OUTPUT@', '@INPUT@'],
      build_by_default: false,
    )
    alldocs += maintable_txt
  endif
endif


#
# Man pages
#
if docs_dep.found()
  # FIXME: implement / consider sqlmansectnum logic
  man = custom_target('man',
    input: ['stylesheet-man.xsl', maintable_full_xml],
    output: ['man1', 'man3', 'man7'],
    depfile: 'man.d',
    command: [xsltproc, '-o', '@OUTDIR@/', xsltproc_html_flags, '@INPUT@'],
    build_by_default: false,
  )
  alldocs += man

  install_doc_man = custom_target('install-man',
    output: 'install-man',
    input: man,
    command: [
      python, install_files, '--prefix', dir_prefix,
      '--install-dirs', dir_man, '@INPUT@'],
    build_always_stale: true, build_by_default: false,
  )
  alias_target('man', man)
  alias_target('install-man', install_doc_man)

  # built and installed as part of the docs target
  installdocs += install_doc_man
  docs += man
endif


#
# Full documentation as PDF
#
if docs_pdf_dep.found()
  xsltproc_fo_flags = xsltproc_flags + [
    '--stringparam', 'img.src.path', meson.current_source_dir() + '/'
  ]

  foreach format, detail: {'A4': 'A4', 'US': 'USletter'}
    maintable_x_fo_f = 'maintable-@0@.fo'.format(format)
    maintable_x_fo_dep = 'maintable-@0@.fo.d'.format(format)
    maintable_x_pdf_f = 'maintable-@0@.pdf'.format(format)

    maintable_x_fo = custom_target(maintable_x_fo_f,
      input: ['stylesheet-fo.xsl', maintable_full_xml],
      output: maintable_x_fo_f,
      depfile: maintable_x_fo_dep,
      command: [xsltproc, xsltproc_fo_flags,
                '--stringparam', 'paper.type', detail,
                '-o', '@OUTPUT@', '@INPUT@'],
      build_by_default: false,
    )

    maintable_x_pdf = custom_target(maintable_x_pdf_f,
      input: [maintable_x_fo],
      output: [maintable_x_pdf_f],
      command: [fop, '-fo', '@INPUT@', '-pdf', '@OUTPUT@'],
      build_by_default: false,
    )
    alldocs += maintable_x_pdf
  endforeach
endif


#
# epub
#

# This was previously implemented using dbtoepub - but that doesn't seem to
# support running in build != source directory (i.e. VPATH builds already
# weren't supported).
if pandoc.found() and xsltproc_bin.found()
  maintable_epub = custom_target('maintable.epub',
    input: maintable_full_xml,
    output: 'maintable.epub',
    command: [pandoc, '-f', 'docbook', '-t', 'epub', '-o', '@OUTPUT@', '--resource-path=@CURRENT_SOURCE_DIR@',
              '@INPUT@'],
    build_by_default: false,
  )
  alldocs += maintable_epub
endif


##
## Experimental Texinfo targets
##

db2x_xsltproc = find_program('db2x_xsltproc', native: true, required: false)
db2x_texixml = find_program('db2x_texixml', native: true, required: false)
makeinfo = find_program('makeinfo', native: true, required: false)

if xsltproc_bin.found() and db2x_texixml.found() and db2x_xsltproc.found() and makeinfo.found()
  maintable_texixml = custom_target('maintable.texixml',
    output: 'maintable.texixml',
    command: [db2x_xsltproc, '-s', 'texi',
              '-g', 'output-file=maintable',
              maintable_full_xml, '-o', '@OUTPUT@'],
    build_by_default: false,
  )
  maintable_texi = custom_target('maintable.texi',
    output: 'maintable.texi',
    command: [db2x_texixml, '--encoding=utf-8', maintable_texixml,
              '--output-dir=@OUTDIR@'],
    build_by_default: false,
  )
  maintable_info = custom_target('maintable.info',
    output: 'maintable.info',
    command: [makeinfo, '--enable-encoding', '--no-split', '--no-validate',
              maintable_texi, '-o', '@OUTPUT0@'],
    build_by_default: false,
  )
  alldocs += maintable_info
endif


if docs_dep.found()
  alias_target('docs', docs)
  alias_target('install-docs', installdocs)
endif

if alldocs.length() != 0
  alias_target('alldocs', alldocs)
endif
