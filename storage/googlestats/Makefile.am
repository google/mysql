# Copyright (C) 2005-2006 MySQL AB
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA

#called from the top level Makefile

MYSQLDATAdir =          $(localstatedir)
MYSQLSHAREdir =         $(pkgdatadir)
MYSQLBASEdir=           $(prefix)
MYSQLLIBdir=            $(pkglibdir)
INCLUDES =              -I$(top_srcdir)/include -I$(top_builddir)/include \
                        -I$(top_srcdir)/regex \
                        -I$(top_srcdir)/sql \
                        $(LZO_INCLUDES) -I$(srcdir)
WRAPLIBS=

LDADD =

DEFS =                  -DMYSQL_SERVER @DEFS@

noinst_HEADERS =	gss_aux.h gss_cache.h ha_googlestats.h

EXTRA_LTLIBRARIES =	ha_googlestats.la
pkglib_LTLIBRARIES =	@plugin_googlestats_shared_target@
ha_googlestats_la_LDFLAGS =	-module -rpath $(MYSQLLIBdir)
ha_googlestats_la_CXXFLAGS=	$(AM_CXXFLAGS) -DMYSQL_DYNAMIC_PLUGIN
ha_googlestats_la_CFLAGS =	$(AM_CFLAGS) -DMYSQL_DYNAMIC_PLUGIN
ha_googlestats_la_SOURCES =	gss_aux.cc gss_cache.cc ha_googlestats.cc


EXTRA_LIBRARIES =	libgooglestats.a
noinst_LIBRARIES =	@plugin_googlestats_static_target@
libgooglestats_a_CXXFLAGS =	$(AM_CXXFLAGS)
libgooglestats_a_CFLAGS =	$(AM_CFLAGS)
libgooglestats_a_SOURCES=	gss_aux.cc  gss_cache.cc ha_googlestats.cc


EXTRA_DIST =		CMakeLists.txt plug.in
# Don't update the files from bitkeeper
%::SCCS/s.%
