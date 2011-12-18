# This script contains the autogen/configure/make part of the
# build. It is included in several executables (build, deb, mpm, etc)
# and it assumes a whole set of environmental variables are properly
# set.

# Autogen

${ACLOCAL}
autoheader
libtoolize --automake --force --copy
${AUTOMAKE} --foreign --add-missing --copy
autoconf

# Configure
pushd ./

cd ${BUILD_DIR}
set -x
${SRC_DIR}/configure \
  --host=${GNU_TARGET} \
  --enable-assembler \
  --enable-local-infile \
  --enable-profiling \
  --with-big-tables \
  --without-manager \
  --with-extra-charsets=all \
  --with-mysqld-user=mysql \
  --with-unix-socket-path=/var/lib/mysql/mysql.sock \
  --with-pic \
  --prefix=/ \
  --exec-prefix=/usr \
  --libexecdir=/usr/sbin \
  --libdir=/usr/lib \
  --sysconfdir=/etc \
  --datadir=/usr/share \
  --localstatedir=/var/lib/mysql \
  --infodir=/usr/share/info \
  --includedir=/usr/include \
  --mandir=/usr/share/man \
  --enable-thread-safe-client \
  --with-plugins=innodb_plugin \
  --with-lzo2-dir=bundled \
  --with-ncurses-dir=bundled \
  --with-readline \
  --with-perftools-dir=bundled \
  ${enable_perftools_profiling} \
  --enable-perftools-tcmalloc \
  ${enable_minimal} \
  --with-comment="Google MySQL ${MYSQL_VERSION} build ${MYSQL_RELEASE}" \
  --disable-shared \
  --with-zlib-dir=bundled \
  --with-server-suffix=${server_suffix} \
  --without-embedded-server \
  --without-vio \
  ${with_debug} \
  --without-docs \
  --with-man \
  CC=${CC} CPP=${CPP} CXX=${CXX} LD=${LD} RANLIB=${RANLIB} AR=${AR} \
  CPPFLAGS="${CPPFLAGS}" \
  LDFLAGS="${LDFLAGS} ${profile_lib}" \
  STRIP="/bin/true" \
  CFLAGS="${CFLAGS} ${profile_opt} ${ncurses_inc}" \
  CXXFLAGS="${CXXFLAGS} ${profile_opt} ${ncurses_inc}" \
  GETCONF="${GETCONF_BIN}"
set -x

# Build

make ${MAKEFLAGS}

popd
