dnl Define lzo2 paths to point at bundled lzo2
AC_DEFUN([GOOGLE_USE_BUNDLED_LZO2], [
LZO_INCLUDES="-I\$(top_srcdir)/lzo/include/lzo"
LZO_LIBS="\$(top_builddir)/lzo/src/liblzo2.la"
dnl Omit -L$pkglibdir as it's always in the list of mysql_config deps.
LZO_DEPS="-llzo2"
lzo_dir="lzo"
AC_SUBST([lzo_dir])
mysql_cv_lzo2="yes"
])

dnl Auxiliary macro to check for lzo2 at given path
AC_DEFUN([GOOGLE_CHECK_LZO2_DIR], [
save_CPPFLAGS="$CPPFLAGS"
save_LIBS="$LIBS"
CPPFLAGS="$LZO_INCLUDES $CPPFLAGS"
LIBS="$LIBS $LZO_LIBS"
AC_CACHE_VAL([mysql_cv_lzo2],
  [AC_TRY_LINK([#include <lzoconf.h>],
    [return lzo_version();],
    [mysql_cv_lzo2="yes"
    AC_MSG_RESULT([ok])],
    [mysql_cv_lzo2="no"])
  ])
CPPFLAGS="$save_CPPFLAGS"
LIBS="$save_LIBS"
])

dnl GOOGLE_CHECK_LZO2
dnl ------------------------------------------------------------------------
dnl @synopsis GOOGLE_CHECK_LZO2
dnl
dnl Provides the following configure options:
dnl --with-lzo2-dir=DIR
dnl Possible DIR values are:
dnl - "no" - the macro will disable use of compression functions
dnl - "bundled" - means use lzo2 bundled along with MySQL sources
dnl - empty, or not specified - the macro will try default system
dnl   library (if present), and in case of error will fall back to 
dnl   bundled lzo2
dnl - lzo2 location prefix - given location prefix, the macro expects
dnl   to find the library headers in $prefix/include, and binaries in
dnl   $prefix/lib. If lzo2 headers or binaries weren't found at $prefix, the
dnl   macro bails out with error.
dnl 
dnl If the library was found, this function #defines HAVE_LZO2
dnl and configure variables LZO_INCLUDES (i.e. -I/path/to/lzo2/include),
dnl LZO_LIBS (i. e. -L/path/to/lzo2/lib -lz) and LZO_DEPS which is
dnl used in mysql_config and is always the same as LZO_LIBS except to
dnl when we use the bundled lzo2. In the latter case LZO_LIBS points to the
dnl build dir ($top_builddir/lzo2), while mysql_config must point to the
dnl installation dir ($pkglibdir), so LZO_DEPS is set to point to
dnl $pkglibdir.

AC_DEFUN([GOOGLE_CHECK_LZO2], [
AC_MSG_CHECKING([for lzo2 compression library])
AC_ARG_WITH([lzo2-dir],
            AC_HELP_STRING([--with-lzo2-dir=DIR],
                           [Provide MySQL with a custom location of
                           the lzo2 library. Given DIR, lzo2 library
                           is assumed to be in $DIR/lib and header files
                           in $DIR/include.]),
            [mysql_lzo2_dir=${withval}],
            [mysql_lzo2_dir=""])
mysql_cv_local_lzo2="no"
case "$mysql_lzo2_dir" in
  "no")
    mysql_cv_lzo2="no"
    AC_MSG_RESULT([disabled])
    ;;
  "bundled")
    GOOGLE_USE_BUNDLED_LZO2
    AC_MSG_RESULT([using bundled lzo2])
    mysql_cv_local_lzo2="yes"
    ;;
  "")
    LZO_INCLUDES=""
    LZO_LIBS="-llzo2"
    GOOGLE_CHECK_LZO2_DIR
    if test "$mysql_cv_lzo2" = "no"; then
      GOOGLE_USE_BUNDLED_LZO2
      AC_MSG_RESULT([system-wide lzo2 not found, using one bundled with MySQL])
      mysql_cv_local_lzo2="yes"
    fi
    ;;
  *)
    # Test for libz using all known library file endings
    if test \( -f "$mysql_lzo2_dir/lib/liblzo2.a"  -o \
               -f "$mysql_lzo2_dir/lib/liblzo2.so" -o \
               -f "$mysql_lzo2_dir/lib/liblzo2.sl" -o \
               -f "$mysql_lzo2_dir/lib/liblzo2.dylib" \) \
            -a -f "$mysql_lzo2_dir/include/lzo/lzo1.h"; then
      LZO_INCLUDES="-I$mysql_lzo2_dir/include/lzo"
      LZO_LIBS="-L$mysql_lzo2_dir/lib -llzo2"
      GOOGLE_CHECK_LZO2_DIR
    fi
    if test "x$mysql_cv_lzo2" != "xyes"; then 
      AC_MSG_ERROR([headers or binaries were not found in $mysql_lzo2_dir/{include,lib}])
    fi
    mysql_cv_local_lzo2="no"
    ;;
esac
if test "$mysql_cv_lzo2" = "yes"; then
  if test "$mysql_cv_local_lzo2" = "yes"; then
    AC_CONFIG_SUBDIRS(lzo)
  fi
  if test "x$LZO_DEPS" = "x"; then
    LZO_DEPS="$LZO_LIBS"
  fi
  AC_SUBST([LZO_LIBS])
  AC_SUBST([LZO_DEPS])
  AC_SUBST([LZO_INCLUDES])
  AC_DEFINE([HAVE_LZO2], [1], [Define to enable lzo2 support])
fi
])

dnl ------------------------------------------------------------------------
