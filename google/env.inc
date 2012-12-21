# Setup the environment variables

source VERSION

MYSQL_VERSION="${MYSQL_VERSION_MAJOR}.${MYSQL_VERSION_MINOR}.${MYSQL_VERSION_PATCH}"
MYSQL_RELEASE=$(date +%Y%m%d.%H%M%S.$(git rev-parse --short HEAD))

if [[ -a google/env-google.inc ]]
then
  source google/env-google.inc
else
  ACLOCAL="aclocal"
  AUTOMAKE="automake"

  if [[ "$GOOGLE_CLANG" = YES ]]
  then
    CC="clang"
    CXX="clang++"
  else
    CC="gcc"
    CXX="g++"
  fi
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

BUILD_DIR="$(pwd)"
SRC_DIR="$(pwd)"

NUM_CPUS=$(grep -c 'processor.*:' /proc/cpuinfo)
NUM_CPUS=$(echo ${NUM_CPUS} \* 2 | bc)
: ${MAKEFLAGS:="-j ${NUM_CPUS} -s"}

if [[ -n "${GOOGLE_DEBUG}" ]]
then
  with_debug="-DCMAKE_BUILD_TYPE=Debug"
  release_suffix="debug"
else
  release_suffix="prod"
fi

if [[ -n "${GOOGLE_VALGRIND}" ]]
then
  VALGRIND_FLAGS="-USAFEMALLOC -UFORCE_INIT_OF_VARS -DHAVE_purify"
  CFLAGS="${CFLAGS} ${VALGRIND_FLAGS}"
  CXXFLAGS="${CXXFLAGS} ${VALGRIND_FLAGS}"
  release_suffix="valgrind"
fi
