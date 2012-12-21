# This script contains the cmake/make part of the build.
# It is included in several executables (compile, deb, etc) and
# it assumes a whole set of environmental variables are properly set.

set -x

common_compiler_flags="-fno-omit-frame-pointer -funsigned-char"

CFLAGS="${CFLAGS} ${common_compiler_flags}"
CXXFLAGS="${CXXFLAGS} ${common_compiler_flags}"
CPPFLAGS="${CPPFLAGS} -DBIG_TABLES -DMYSQL_SERVER_SUFFIX=-google -Werror"
LDFLAGS="${LDFLAGS}"
STRIP="/bin/true"
GETCONF="${GETCONF_BIN}"

export CC
export CXX
export RANLIB
export AR
export CFLAGS
export CPPFLAGS
export CXXFLAGS
export LDFLAGS
export STRIP
export GETCONF

cmake . -DWITH_READLINE=system \
        -DWITH_ZLIB=system \
        -DWITH_JEMALLOC=OFF \
        -DWITH_FEDERATEDX=ON \
        -DWITH_PERFSCHEMA_STORAGE_ENGINE=OFF \
        -DWITHOUT_XTRADB_STORAGE_ENGINE=ON \
        -DWITH_INNOBASE_STORAGE_ENGINE=ON \
        -DWITH_SEMISYNC_MASTER=ON \
        -DWITH_SEMISYNC_SLAVE=ON \
        -DMYSQL_MAINTAINER_MODE=ON \
        -DWITH_PIC=ON \
        -DCOMPILATION_COMMENT="Google MySQL ${MYSQL_VERSION} build ${MYSQL_RELEASE}" \
        -DENABLED_LOCAL_INFILE=ON \
        -DENABLED_PROFILING=OFF \
        -DCMAKE_INSTALL_PREFIX="/usr" \
        -DWITH_EMBEDDED_SERVER=ON \
        ${with_debug}

make ${MAKEFLAGS}
