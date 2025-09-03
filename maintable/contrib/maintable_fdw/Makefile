# contrib/maintable_fdw/Makefile

MODULE_big = maintable_fdw
OBJS = \
	$(WIN32RES) \
	connection.o \
	deparse.o \
	option.o \
	maintable_fdw.o \
	shippable.o
PGFILEDESC = "maintable_fdw - foreign data wrapper for maintableQL"

PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK_INTERNAL = $(libpq)

EXTENSION = maintable_fdw
DATA = maintable_fdw--1.0.sql maintable_fdw--1.0--1.1.sql maintable_fdw--1.1--1.2.sql

REGRESS = maintable_fdw query_cancel
TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
SHLIB_PREREQS = submake-libpq
subdir = contrib/maintable_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
