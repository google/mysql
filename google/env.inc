# Setup the environment variables

MYSQL_VERSION=$(grep 'AC_INIT.*[.0-9]*' configure.in | sed -r 's/.*(5\.[0-9]+\.[0-9]+).*/\1/')
MYSQL_RELEASE=$(date +%Y%m%d.%H%M%S.$(git rev-parse --short HEAD))

if [[ -a google/env-google.inc ]]
then
  source google/env-google.inc
else
  ACLOCAL="aclocal"
  AUTOMAKE="automake"

  CC="gcc"
  CPP="cpp"
  CXX="g++"
  LD="ld"
  AR="ar"
  RANLIB="ranlib"

  LDFLAGS="${LDFLAGS} -lrt"

  if [[ -n "${GOOGLE_DEBUG}" ]]
  then
    CFLAGS="-g3 -O0"
    CXXFLAGS="-g3 -O0"
  else
    CFLAGS="-g2 -O2 -fno-strict-aliasing"
    CXXFLAGS="-g2 -O2 -fno-strict-aliasing"
  fi
fi

: ${TMPDIR:="/tmp"}
SRC_DIR=$(pwd)
: ${BUILD_DIR:=$(pwd)}
[[ -d ${BUILD_DIR} ]] || mkdir -p ${BUILD_DIR}
rm -f build
ln -s ${BUILD_DIR} build
NUM_CPUS=$(grep -c 'processor.*:' /proc/cpuinfo)
NUM_CPUS=$(echo ${NUM_CPUS} \* 2 | bc)
: ${MAKEFLAGS:="-j ${NUM_CPUS} -s"}

server_suffix="-google"
if [[ -n "${GOOGLE_DEBUG}" ]]
then
  release_suffix="debug"
  with_debug="--with-debug"
else
  release_suffix="prod"
  with_debug="--without-debug"
fi

if [[ -n "${GOOGLE_PROFILE}" ]]
then
  server_suffix="${server_suffix}-profile"
  release_suffix="profile"
  enable_perftools_profiling="--enable-perftools-profiling"
fi

if [[ -n "${GOOGLE_VALGRIND}" ]]
then
  server_suffix="${server_suffix}-valgrind"
  release_suffix="valgrind"
  VALGRIND_FLAGS="-USAFEMALLOC -UFORCE_INIT_OF_VARS -DHAVE_purify"
  CFLAGS="${CFLAGS} ${VALGRIND_FLAGS}"
  CXXFLAGS="${CXXFLAGS} ${VALGRIND_FLAGS}"
fi

# For now, always use this to make core files easier to inspect
profile_opt="-fno-omit-frame-pointer"

# cmd-line-utils/libedit/term.c needs to see the declaration of tgetstr,
# which is extern declared in ncurses/include/term.h,
# that is generated during ncurses build, long after config.h is produced.
# So we need to add -DHAVE_TERM_H there in order to see term.h
ncurses_inc="-I${BUILD_DIR}/ncurses/include -I${SRC_DIR}/ncurses/include -DHAVE_TERM_H"
