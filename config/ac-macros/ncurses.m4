dnl Define ncurses paths to point at bundled ncurses
AC_DEFUN([GOOGLE_USE_BUNDLED_NCURSES], [
NCURSES_INCLUDES="-I\$(top_srcdir)/ncurses/include"
NCURSES_LIBS="\$(top_builddir)/ncurses/lib/libncurses.a"
NCURSES_DEPS="-lncurses"
ncurses_dir="ncurses"
AC_SUBST([ncurses_dir])
mysql_cv_ncurses="yes"
])

dnl Auxiliary macro to check for ncurses at given path
AC_DEFUN([GOOGLE_CHECK_NCURSES_DIR], [
save_CPPFLAGS="$CPPFLAGS"
save_LIBS="$LIBS"
CPPFLAGS="$NCURSES_INCLUDES $CPPFLAGS"
LIBS="$LIBS $NCURSES_LIBS"
AC_CACHE_VAL([mysql_cv_ncurses],
  [AC_TRY_LINK([#include <ncurses.h>],
    [return termattrs();],
    [mysql_cv_ncurses="yes"
    AC_MSG_RESULT([ok])],
    [mysql_cv_ncurses="no"])
  ])
CPPFLAGS="$save_CPPFLAGS"
LIBS="$save_LIBS"
])

dnl GOOGLE_CHECK_NCURSES
dnl ------------------------------------------------------------------------
dnl @synopsis GOOGLE_CHECK_NCURSES
dnl
dnl Provides the following configure options:
dnl --with-ncurses-dir=DIR
dnl Possible DIR values are:
dnl - "no" - the macro will disable use of compression functions
dnl - "bundled" - means use ncurses bundled along with MySQL sources
dnl - empty, or not specified - the macro will try default system
dnl   library (if present), and in case of error will fall back to 
dnl   bundled ncurses
dnl - ncurses location prefix - given location prefix, the macro expects
dnl   to find the library headers in $prefix/include, and binaries in
dnl   $prefix/lib. If ncurses headers or binaries weren't found at $prefix, the
dnl   macro bails out with error.
dnl 
dnl If the library was found, this function #defines HAVE_NCURSES
dnl and configure variables NCURSES_INCLUDES (i.e. -I/path/to/ncurses/include),
dnl NCURSES_LIBS (i. e. -L/path/to/ncurses/lib -lz) and NCURSES_DEPS which is
dnl used in mysql_config and is always the same as NCURSES_LIBS except to
dnl when we use the bundled ncurses. In the latter case NCURSES_LIBS points to the
dnl build dir ($top_builddir/ncurses), while mysql_config must point to the
dnl installation dir ($pkglibdir), so NCURSES_DEPS is set to point to
dnl $pkglibdir.

AC_DEFUN([GOOGLE_CHECK_NCURSES], [
AC_MSG_CHECKING([for ncurses terminal library])
AC_ARG_WITH([ncurses-dir],
            AC_HELP_STRING([--with-ncurses-dir=DIR],
                           [Provide MySQL with a custom location of
                           the ncurses library. Given DIR, ncurses library
                           is assumed to be in $DIR/lib and header files
                           in $DIR/include.]),
            [mysql_ncurses_dir=${withval}],
            [mysql_ncurses_dir=""])
mysql_cv_local_ncurses="no"
case "$mysql_ncurses_dir" in
  "no")
    mysql_cv_ncurses="no"
    AC_MSG_RESULT([disabled])
    ;;
  "bundled")
    GOOGLE_USE_BUNDLED_NCURSES
    AC_MSG_RESULT([using bundled ncurses])
    mysql_cv_local_ncurses="yes"
    ;;
  "")
    NCURSES_INCLUDES=""
    NCURSES_LIBS="-lncurses"
    GOOGLE_CHECK_NCURSES_DIR
    if test "x$mysql_cv_ncurses" = "xno"; then
      GOOGLE_USE_BUNDLED_NCURSES
      AC_MSG_RESULT([system-wide ncurses not found, using one bundled with MySQL])
      mysql_cv_local_ncurses="yes"
    fi
    ;;
  *)
    # Test for libncurses using all known library file endings
    if test \( -f "$mysql_ncurses_dir/lib/libncurses.a"  -o \
               -f "$mysql_ncurses_dir/lib/libncurses.so" -o \
               -f "$mysql_ncurses_dir/lib/libncurses.sl" -o \
               -f "$mysql_ncurses_dir/lib/libncurses.dylib" \) \
            -a -f "$mysql_ncurses_dir/include/ncurses.h"; then
      NCURSES_INCLUDES="-I$mysql_ncurses_dir/include"
      NCURSES_LIBS="-L$mysql_ncurses_dir/lib -lncurses"
      GOOGLE_CHECK_NCURSES_DIR
    fi
    if test "x$mysql_cv_ncurses" != "xyes"; then 
      AC_MSG_ERROR([headers or binaries were not found in $mysql_ncurses_dir/{include,lib}])
    fi
    ;;
esac
if test "$mysql_cv_ncurses" = "yes"; then
  if test "$mysql_cv_local_ncurses" = "yes"; then
    AC_CONFIG_SUBDIRS(ncurses)
  fi
  if test "x$NCURSES_DEPS" = "x"; then
    NCURSES_DEPS="$NCURSES_LIBS"
  fi
  AC_SUBST([NCURSES_LIBS])
  AC_SUBST([NCURSES_DEPS])
  AC_SUBST([NCURSES_INCLUDES])
  AC_DEFINE([HAVE_NCURSES], [1], [Define to enable ncurses support])
fi
])

dnl ------------------------------------------------------------------------
