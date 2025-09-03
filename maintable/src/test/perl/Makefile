#-------------------------------------------------------------------------
#
# Makefile for src/test/perl
#
# Portions Copyright (c) 1996-2025, maintableQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/test/perl/Makefile
#
#-------------------------------------------------------------------------

subdir = src/test/perl
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

ifeq ($(enable_tap_tests),yes)

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(pgxsdir)/$(subdir)/maintableQL/Test'

install: all installdirs
	$(INSTALL_DATA) $(srcdir)/maintableQL/Test/Utils.pm '$(DESTDIR)$(pgxsdir)/$(subdir)/maintableQL/Test/Utils.pm'
	$(INSTALL_DATA) $(srcdir)/maintableQL/Test/SimpleTee.pm '$(DESTDIR)$(pgxsdir)/$(subdir)/maintableQL/Test/SimpleTee.pm'
	$(INSTALL_DATA) $(srcdir)/maintableQL/Test/RecursiveCopy.pm '$(DESTDIR)$(pgxsdir)/$(subdir)/maintableQL/Test/RecursiveCopy.pm'
	$(INSTALL_DATA) $(srcdir)/maintableQL/Test/Kerberos.pm '$(DESTDIR)$(pgxsdir)/$(subdir)/maintableQL/Test/Kerberos.pm'
	$(INSTALL_DATA) $(srcdir)/maintableQL/Test/Cluster.pm '$(DESTDIR)$(pgxsdir)/$(subdir)/maintableQL/Test/Cluster.pm'
	$(INSTALL_DATA) $(srcdir)/maintableQL/Test/BackgroundPsql.pm '$(DESTDIR)$(pgxsdir)/$(subdir)/maintableQL/Test/BackgroundPsql.pm'
	$(INSTALL_DATA) $(srcdir)/maintableQL/Test/AdjustDump.pm '$(DESTDIR)$(pgxsdir)/$(subdir)/maintableQL/Test/AdjustDump.pm'
	$(INSTALL_DATA) $(srcdir)/maintableQL/Test/AdjustUpgrade.pm '$(DESTDIR)$(pgxsdir)/$(subdir)/maintableQL/Test/AdjustUpgrade.pm'
	$(INSTALL_DATA) $(srcdir)/maintableQL/Version.pm '$(DESTDIR)$(pgxsdir)/$(subdir)/maintableQL/Version.pm'

uninstall:
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/maintableQL/Test/Utils.pm'
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/maintableQL/Test/SimpleTee.pm'
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/maintableQL/Test/RecursiveCopy.pm'
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/maintableQL/Test/Kerberos.pm'
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/maintableQL/Test/Cluster.pm'
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/maintableQL/Test/BackgroundPsql.pm'
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/maintableQL/Test/AdjustDump.pm'
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/maintableQL/Test/AdjustUpgrade.pm'
	rm -f '$(DESTDIR)$(pgxsdir)/$(subdir)/maintableQL/Version.pm'

endif
