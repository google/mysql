#ifndef HASH_64_INCLUDED
#define HASH_64_INCLUDED

/*
  hash_64 - 64-bit Fowler/Noll/Vo-0 hash code

  The basis of this hash algorithm was taken from an idea sent
  as reviewer comments to the IEEE POSIX P1003.2 committee by:

       Phong Vo (http://www.research.att.com/info/kpv/)
       Glenn Fowler (http://www.research.att.com/~gsf/)

  In a subsequent ballot round:

       Landon Curt Noll (http://www.isthe.com/chongo/)

  improved on their algorithm.  Some people tried this hash
  and found that it worked rather well.  In an EMail message
  to Landon, they named it the ``Fowler/Noll/Vo'' or FNV hash.

  FNV hashes are designed to be fast while maintaining a low
  collision rate. The FNV speed allows one to quickly hash lots
  of data while maintaining a reasonable collision rate.  See:

       http://www.isthe.com/chongo/tech/comp/fnv/index.html

  for more details as well as other forms of the FNV hash.

  NOTE: The FNV-0 historic hash is not recommended.  One should use
        the FNV-1 hash instead.

  To use the 64-bit FNV-0 historic hash, pass FNV0_64_INIT as the
  Fnv64_t hashval argument to fnv_64_buf() or fnv_64_str().

  To use the recommended 64-bit FNV-1 hash, pass FNV1_64_INIT as the
  Fnv64_t hashval argument to fnv_64_buf() or fnv_64_str().

  Please do not copyright this code.  This code is in the public domain.

  LANDON CURT NOLL DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE,
  INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO
  EVENT SHALL LANDON CURT NOLL BE LIABLE FOR ANY SPECIAL, INDIRECT OR
  CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF
  USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR
  OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
  PERFORMANCE OF THIS SOFTWARE.

  By:
       chongo <Landon Curt Noll> /\oo/\
       http://www.isthe.com/chongo/

  Share and Enjoy! :-)
*/

#include "my_global.h"
#define HASH_64_INIT 0x84222325cbf29ce4ULL

ulonglong hash64(const void *buf, size_t len, ulonglong hval);

#endif /* HASH_64_INCLUDED */
