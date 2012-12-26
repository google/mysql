/*
  This file contains a wrapper around IO operations which checksums
  the data written and read to disk. This all happens transparently
  to the caller, so as long as the caller uses only chksum_*
  functions, the file that they see will look as if it were a normal
  file.

  The underlying implementation of the chksum_* functions does
  block-level checksumming, where the block size is defined by
  IOCACHE_BLOCK_SIZE. Because we include some header information with
  each block (such as the checksum), IOCACHE_FITTED_BLOCK_SIZE has
  been defined as the amount of space available in each block for
  data storage. This in turn is based off of the struct
  `chksum_block`. When modifying chksum_block, make sure to adjust
  the size of IOCACHE_FITTED_BLOCK_SIZE to match.
*/

#include "mysys_priv.h"
#include <m_string.h>

/* Size of checksummed blocks when writing to disk */
#define IOCACHE_BLOCK_SIZE        4096

/* Used to align sizeof(chksum_block) with IOCACHE_BLOCK_SIZE */
/* If you need to change chksum_block, you need to change this #define as well */
#define IOCACHE_FITTED_BLOCK_SIZE (IOCACHE_BLOCK_SIZE - sizeof(ha_checksum))

/* don't change this! */
#define IOCACHE_BLOCK_HEADER_SIZE (IOCACHE_BLOCK_SIZE - IOCACHE_FITTED_BLOCK_SIZE)

/* Get the (zero-indexed) block number counted from the beginning of the file */
/* n is the offset from beginning of file */
#define get_block_num(n) ((n) / IOCACHE_FITTED_BLOCK_SIZE)
/* Get the byte offset from the beginning of the data section for the block we are in */
/* n is the offset from beginning of file */
#define get_block_offset(n) ((n) % IOCACHE_FITTED_BLOCK_SIZE)
/* Get the location of the start of our block */
/* n is the index of the block in the file (zero-indexed) */
#define get_block_loc(index) (((my_off_t) index) * IOCACHE_BLOCK_SIZE)

/*
  When reading this struct to and from disk, read in increments of
  IOCACHE_BLOCK_SIZE. This will read the chksum field and the data
  field. If this block read completely, then we are in the middle of
  the file and should manually set len to IOCACHE_FITTED_BLOCK_SIZE.
  If this block is not read completely, set it to the read length minus
  the IOCACHE_BLOCK_HEADER_SIZE.
*/
typedef struct chksum_block
{
  ha_checksum chksum;
  /* Change IOCACHE_FITTED_BLOCK_SIZE when modifying this struct */
  uchar       data[IOCACHE_FITTED_BLOCK_SIZE];
  uint32      len;
} CHKSUM_BLOCK;

/*
  Convert fake addressing to the real address in the file
  Fake addressing ignores block headers when computing offsets

  @param[in] pos The fake address

  @return the corresponding 'real' address
*/
static size_t chksum_pos_ftor(size_t pos)
{
  size_t block_offset= get_block_offset(pos);
  size_t block_num= get_block_num(pos);

  /* We do the last bit to add in the header size. */
  size_t real_pos= (block_num * IOCACHE_BLOCK_SIZE) + block_offset
    + (IOCACHE_BLOCK_HEADER_SIZE * (block_offset != 0));

  return real_pos;
}

/*
  Convert real addressing to the fake address that MySQL sees

  @param[in] pos The real address

  @return the corresponding fake address
*/
static size_t chksum_pos_rtof(size_t pos)
{
  size_t block_offset= pos % IOCACHE_BLOCK_SIZE;
  size_t block_num= pos / IOCACHE_BLOCK_SIZE;
  size_t fake_pos;

  /*
    if we are given a real pos inside a block header, align to the
    beginning of the block data
  */
  if (block_offset == 0)
    block_offset+= IOCACHE_BLOCK_HEADER_SIZE;

  /* We do the last bit to account for the header size in the last block */
  fake_pos= block_num * IOCACHE_FITTED_BLOCK_SIZE + block_offset
    - IOCACHE_BLOCK_HEADER_SIZE;

  return fake_pos;
}


/*
  Copy a section of our Buffer into the given block

  This function copies data into the block and recalculates
  the requisite chksum and len.

  @param[in,out]  block   The chksum_block we will be writing our data into
  @param[in]      Buffer  The data we will be copying into the block
  @param[in]      start   The offset from the beginning of the block to which
                          we should begin copying the data
  @param[in]      count   The total number of bytes that are left copied from
                          Buffer

  @return length written on success, -1 on failure
*/
static int insert_into_block(CHKSUM_BLOCK *block,  const uchar *Buffer,
                      my_off_t start, my_off_t count)
{
  int to_write;

  if (start > IOCACHE_FITTED_BLOCK_SIZE)
    return -1;

  if (start + count > IOCACHE_FITTED_BLOCK_SIZE)
    to_write= IOCACHE_FITTED_BLOCK_SIZE - start;
  else
    to_write= count;

  if (block->len < start + to_write)
    block->len= start + to_write;

  memcpy(block->data + start, Buffer, to_write);

  block->chksum= my_checksum(0L, block->data, block->len);

  return to_write;
}

/*
  Write the given CHKSUM_BLOCK to the specified file.

  @param[in] file       The file we are writing to
  @param[in] block      The block we are writing to disk
  @param[in] block_num  The block index (zero-indexed)

  @return number of bytes written, or -1 on failure
  Should only ever return IOCACHE_BLOCK_SIZE
*/
static size_t chksum_write_block(int file, CHKSUM_BLOCK *block, int block_num)
{
  my_off_t loc;
  size_t res;

  loc= get_block_loc(block_num);

  DBUG_ASSERT(block->len <= IOCACHE_FITTED_BLOCK_SIZE);
  res= my_pwrite(file, (void *) block, block->len + IOCACHE_BLOCK_HEADER_SIZE,
                 loc, MYF(0));

  return res;
}

/*
  Retrieve the block at the given index, validating its checksum

  @param[in]    Filedes   The file we should read from
  @param[out]   block     The block we will read our data into
  @param[in]    block_num The index (zero-indexed) of the block we want

  @return
    @retval SUCCESS 0
    @retval ERROR   MY_FILE_ERROR
*/
static size_t get_block_at_index(int Filedes,  CHKSUM_BLOCK *block, int block_num)
{
  my_off_t block_loc;
  ha_checksum chksum;
  size_t res;

  block_loc= get_block_loc(block_num);

  res= my_pread(Filedes, (void *) block, IOCACHE_BLOCK_SIZE,
                block_loc, MYF(0));

  if (res == 0)
  {
    /* if we get here it probably means we're writing to an empty file */
    block->len= 0;
  }
  else if (res <= IOCACHE_BLOCK_SIZE && res > 0)
  {
    block->len= res - IOCACHE_BLOCK_HEADER_SIZE;
    chksum= my_checksum(0L, block->data, block->len);
    if (chksum != block->chksum)
    {
      fprintf(stderr, "Block corruption found in IO Cache: checksums do not match\n");
      fprintf(stderr, "Aborting server...\n");
      exit(1);
    }
  }
  else
    return MY_FILE_ERROR;

  return 0;
}


/*
  Copy data from the given block into Buffer

  @param[in]      block    The block from which we will copy
  @param[out]     Buffer   The buffer which we shall copy to
  @param[in]      start    The offset to begin copying from
  @param[in]      count    the number of bytes left to read into
                           Buffer. If this is greater than the data
                           available, we only read to the end of the block.

  @return
    @retval SUCCESS  number of bytes read
    @retval ERROR    -1
*/
static int read_from_block(CHKSUM_BLOCK *block, uchar *Buffer, my_off_t start,
                    my_off_t count)
{
  int to_read;

  if (start > IOCACHE_FITTED_BLOCK_SIZE)
    return -1;

  if (start + count > IOCACHE_FITTED_BLOCK_SIZE)
    to_read= IOCACHE_FITTED_BLOCK_SIZE - start;
  else
    to_read= count;

  if (to_read + start > block->len)
    return -1;

  memcpy(Buffer, block->data + start, to_read);

  return to_read;
}


/*
  Wraps my_tell with checksum address translation

  @param fd      file to tell
  @param myFlags flags for my_tell

  @return
    @retval SUCCESS the 'fake' location in the file
    @retval ERROR   MY_FILEPOS_ERROR
*/
my_off_t chksum_tell(File fd, myf MyFlags)
{
  size_t real_pos;
  DBUG_ENTER("chksum_tell");

  real_pos= my_tell(fd, MyFlags);

  if (real_pos == MY_FILEPOS_ERROR)
    return real_pos;

  DBUG_RETURN(chksum_pos_rtof(real_pos));
}

/*
  Seek to a position in a file, transparently ignoring chksum headers

  The chksum_seek function is a wrapper around the mysql function my_seek.
  It repositions the  offset of the file descriptor fd to the argument
  offset according to the directive whence as follows:
    SEEK_SET    The offset is set to offset bytes.
    SEEK_CUR    The offset is set to its current location plus offset bytes
    SEEK_END    The offset is set to the size of the file plus offset bytes


  @param fd        The file descriptor
  @param pos       The expected position (absolute or relative)
  @param whence    A direction parameter and one of
                   {SEEK_SET, SEEK_CUR, SEEK_END}
  @param MyFlags   MY_THREADSAFE must be set in case my_seek may be mixed
                   with my_pread/my_pwrite calls and fd is shared among
                   threads.

  @rerturn
    @retval SUCCESS            The new position in the file.
    @retval MY_FILEPOS_ERROR   An error was encountered while performing
                               the seek. my_errno is set to indicate the
                               actual error.
*/
my_off_t chksum_seek(File fd, my_off_t pos, int whence, myf MyFlags)
{
  my_off_t new_pos;
  my_off_t cur_pos;
  my_off_t retval= MY_FILEPOS_ERROR;

  DBUG_ENTER("chksum_seek");
  DBUG_PRINT("chksum", ("fd: %d, pos: %d, whence: %d", (int) fd, (int) pos, (int) whence));

  /* quick test to make sure the pos translation functions are working. */
  /* this should be moved to a test suite test case */
  DBUG_ASSERT(904435 == chksum_pos_ftor(chksum_pos_rtof(904435)));
  DBUG_ASSERT(8192 == chksum_pos_rtof(chksum_pos_ftor(8192)));
  DBUG_ASSERT(24576 == chksum_pos_rtof(chksum_pos_ftor(24576)));
  DBUG_ASSERT(24592 == chksum_pos_rtof(chksum_pos_ftor(24592)));

  switch (whence)
  {
  case SEEK_SET:
    new_pos= chksum_pos_ftor(pos);
    break;
  case SEEK_CUR:
    cur_pos= my_tell(fd, MYF(0));
    /* yeah, it's a bit messy being nested */
    new_pos= chksum_pos_ftor(chksum_pos_rtof(cur_pos) + pos);
    break;
  case SEEK_END:
    retval= my_seek(fd, 0, SEEK_END, MYF(0));
    if (retval == MY_FILEPOS_ERROR)
      return retval;
    /* would probably work better to just use my_stat() */
    new_pos= my_tell(fd, MYF(0));
    if (new_pos == MY_FILEPOS_ERROR)
      return new_pos;

    /* strange handling because we are counting chksum headers backwards */
    new_pos= new_pos + pos + get_block_num(pos)
      * IOCACHE_BLOCK_HEADER_SIZE;
    break;
  default:
    DBUG_PRINT("error", ("chksum_seek didn't recieve a recognized `whence`"));
    DBUG_RETURN(retval);
  }

  retval= my_seek(fd, new_pos, SEEK_SET, MyFlags);
  if (retval != MY_FILEPOS_ERROR)
    retval= chksum_pos_rtof(retval);
  DBUG_RETURN(retval);
}


/* Used only by the test suite */
void test_corrupt_checksum_block(int Filedes)
{
  uchar fake_buffer[IOCACHE_BLOCK_SIZE * 4];
  bzero(fake_buffer, IOCACHE_BLOCK_SIZE * 4);
  my_pwrite(Filedes, fake_buffer, IOCACHE_BLOCK_SIZE * 4, 0, MYF(0));
}

/*
  Wrap my_pread with block-level checksumming

  @param[in]  Filedes   The file to write to
  @param[out] Buffer    The buffer to write from
  @param[in]  Count     The number of bytes to write
  @param[in]  offset    The offset into the file we should begin writing at
  @param[in]  MyFlags   Flags for the underlying my_pread call.

  @return
    @retval SUCCESS  number of bytes read (Unless MY_NABP or MY_FNABP is set)
    @retval ERROR    MY_FILE_ERROR
*/
size_t chksum_pread(File Filedes, uchar *Buffer, size_t Count,
                    my_off_t offset, myf MyFlags)
{
  /* cointaining block for the start of our read */
  CHKSUM_BLOCK block;

  /* doesn't count block metadata */
  size_t bytes_read= 0;

  uint32 block_offset;
  uint32 block_num;

  int read_size;

  size_t cnt;
  size_t res;

  DBUG_ENTER("chksum_pread");
  DBUG_PRINT("chksum", ("Filedes %d, Buffer %p, Count %d",
                        (int) Filedes, (void*) Buffer, (int) Count));

  DBUG_ASSERT(IOCACHE_BLOCK_SIZE == sizeof(CHKSUM_BLOCK)
              - sizeof(block.len));

  DBUG_EXECUTE_IF("chksum_block_corrupted",
                  test_corrupt_checksum_block(Filedes););

  cnt= Count;

  block_num= get_block_num(offset);
  block_offset= get_block_offset(offset);

  while (cnt != 0)
  {
    res= get_block_at_index(Filedes, &block, block_num);

    if (res == MY_FILE_ERROR)
      DBUG_RETURN(res);


    read_size= read_from_block(&block, Buffer, block_offset, cnt);

    if (read_size == -1)
      DBUG_RETURN(read_size);


    block_offset= 0;
    cnt-= read_size;
    Buffer+= read_size;
    bytes_read+= read_size;
    block_num++;
  }

  if (MyFlags & (MY_NABP | MY_FNABP))
    DBUG_RETURN(0);                             /* Want only errors */
  DBUG_RETURN(bytes_read);
}

/*
  Read a chunk of bytes from a file with retries if needed

  The inline checksum data will be stripped out transparently
  Only use this function on data written with chksum_pwrite() or chksum_write()

  @param[in]  Filedes    File descriptor
  @param[out] Buffer     Buffer to hold at least Count bytes
  @param[in]  Count      Bytes to read
  @param[in]  MyFlags    Flags on what to do on error

  @return
    @retval ERROR   -1
    @retval SUCCESS  0  if flag has bits MY_NABP or MY_FNABP set
    @retval SUCCESS  N  number of bytes read.
*/

size_t chksum_read(File Filedes, uchar *Buffer, size_t Count, myf MyFlags)
{
  my_off_t offset;
  size_t bytes_read;

  DBUG_ENTER("chksum_read");
  DBUG_PRINT("chksum", ("Filedes %d, Buffer %p, Count %d",
                        (int) Filedes, (void*) Buffer, (int) Count));

  offset= chksum_tell(Filedes, MYF(0));

  bytes_read= chksum_pread(Filedes, Buffer, Count, offset, MYF(0));

  /* seek to end of write, not end of block */
  chksum_seek(Filedes, offset + bytes_read, MY_SEEK_SET, MyFlags);

  if (MyFlags & (MY_NABP | MY_FNABP))
    DBUG_RETURN(0);			/* Want only errors */
  DBUG_RETURN(bytes_read);
}


/*
  Write a chunk of bytes to a file at a given position

  Blocks of IOCACHE_BLOCK_SIZE will be checksummed inline.
  Be sure to read these files with chksum_read() or chksum_pread()


  @param  Filedes  File decsriptor
  @param  Buffer   Buffer to write data from
  @param  Count    Number of bytes to write
  @param  offset   Position to write to
  @param  MyFlags  Flags

  @return
    @retval ERROR     (size_t) -1
    @retval SUCCESS   Number of bytes read
*/
size_t chksum_pwrite(File Filedes, const uchar *Buffer, size_t Count,
                     my_off_t offset, myf MyFlags)
{
  /* Number of bytes sans checksums. Data only. */
  size_t total_written= 0;
  /* containing block for the start of our read */
  CHKSUM_BLOCK block;

  uint32 block_num;
  uint32 block_offset;

  size_t cnt;
  int written;

  size_t res;

  DBUG_ENTER("chksum_pwrite");
  DBUG_PRINT("chksum", ("Filedes %d, Buffer %p, Count %d, offset %d",
                        (int) Filedes, (void*) Buffer, (int) Count,
                        (int) offset));
  /*
    Determine whether or not we are writing into the middle of a block, and take
    care of that case so that we can be block-aligned.
  */

  cnt= Count;

  block_num= get_block_num(offset);
  block_offset= get_block_offset(offset);

  while (cnt != 0)
  {
    res= get_block_at_index(Filedes, &block, block_num);

    if (res == MY_FILE_ERROR)
      DBUG_RETURN(res);

    written= insert_into_block(&block, Buffer, block_offset, cnt);

    if (written == -1)
      DBUG_RETURN(written);

    res= chksum_write_block(Filedes, &block, block_num);

    if (res == 0 || res == MY_FILE_ERROR)
      DBUG_RETURN(res);

    block_offset= 0;
    cnt-= written;
    Buffer+= written;
    total_written+= written;
    block_num++;
  }

  if (MyFlags & (MY_NABP | MY_FNABP))
    DBUG_RETURN(0);                             /* Want only errors */
  DBUG_RETURN(total_written);
}

/*
  Write a chunk of bytes to a file

  Blocks of IOCACHE_BLOCK_SIZE will be checksummed inline.
  Be sure to read these files with chksum_read() or chksum_pread()

  @param  Filedes  File descriptor
  @param  Buffer   Buffer to hold at least Count bytes
  @param  Count    Bytes to read
  @param  MyFlags  Flags on what to do on error

  @return
    @retval ERROR   -1
    @retval SUCCESS  0  if flag has bits MY_NABP or MY_FNABP set
    @retval SUCCESS  N  number of bytes written.
*/

size_t chksum_write(File Filedes, const uchar *Buffer, size_t Count, myf MyFlags)
{
  my_off_t loc;
  size_t res;
  my_off_t seek_res;

  DBUG_ENTER("chksum_write");
  DBUG_PRINT("chksum", ("Filedes %d, Buffer %p, Count %d",
                        (int) Filedes,(void*) Buffer, (int) Count));
  /*
    determine whether or not we have a previously written block that still
    has space for more data. Lines us up with the block boundary
  */

  loc= chksum_tell(Filedes, MYF(0));

  res= chksum_pwrite(Filedes, Buffer, Count, loc, MYF(0));

  if (res != MY_FILE_ERROR)
  {
    seek_res= chksum_seek(Filedes, loc + Count, MY_SEEK_SET, MYF(0));

    if (seek_res == MY_FILEPOS_ERROR || seek_res != loc + res)
      DBUG_RETURN(-1);
  }

  if (MyFlags & (MY_NABP | MY_FNABP))
    DBUG_RETURN(0);                             /* Want only errors */
  DBUG_RETURN(res);
}

/**********************************************************************
 Testing of checksum_io
**********************************************************************/

#ifdef MAIN

static void die(const char* fmt, ...)
{
  va_list va_args;
  va_start(va_args,fmt);
  fprintf(stderr,"Error:");
  vfprintf(stderr, fmt,va_args);
  fprintf(stderr,", errno=%d\n", errno);
  exit(1);
}

static File open_temp_file(const char* name_prefix)
{
  char fname[FN_REFLEN];
  File fd= create_temp_file(fname, NULL, name_prefix, O_CREAT | O_RDWR,  MYF(0));

  if (fd < 0)
    die("failed to create a temporary file");

  (void) my_delete(fname, MYF(0));
  return fd;
}

void test_chksum_huge_files()
{
  /* this would overflow ints if they are used where they should not */
  const my_off_t file_size= 0x123456000ULL;

  File fd= open_temp_file("chksum_write_huge_test.");

  uchar buf[4096];
  memset(buf, 0xa6, 4096);

  fputs("Testing huge checksummed file writing...\n", stderr);

  my_off_t total_written= 0;
  while (total_written < file_size) {
    size_t to_write= min(file_size - total_written, 4096);
    size_t written= chksum_write(fd, buf, to_write, MYF(0));

    my_off_t chksum_pos= chksum_tell(fd, MYF(0));
    my_off_t real_pos= my_tell(fd, MYF(0));

    if (written == MY_FILE_ERROR || written != to_write)
    {
      fprintf(stderr, "total_written: 0x%08llx chksum_pos: 0x%08llx, real_pos: 0x%08llx\n",
              total_written, chksum_pos, real_pos);
      die("chksum_write returned an error while writing to a file");
    }

    total_written+= written;

    fprintf(stderr, "0x%08llx of 0x%08llx\r", total_written, file_size);

    /* chksum_pos can never be greater than the real_pos, since checksumming
     * has some overhead.  However this overhead is supposed to be small, so
     * chksum_pos cannot be much smaller than the real_pos.
     */
    if (chksum_pos != total_written || chksum_pos < real_pos/2 || chksum_pos > real_pos)
    {
      fprintf(stderr, "total_written: 0x%08llx chksum_pos: 0x%08llx, real_pos: 0x%08llx\n",
              total_written, chksum_pos, real_pos);
      die("chksum I/O position has wrapped due to an improper cast/sign extension");
    }
  }
  chksum_seek(fd, 0, SEEK_SET, MYF(0));

  fputs("Testing huge checksummed file reading...\n", stderr);

  my_off_t total_read= 0;
  while (total_read < total_written) {
    size_t read= chksum_read(fd, buf, 4096, MYF(0));

    my_off_t chksum_pos= chksum_tell(fd, MYF(0));
    my_off_t real_pos= my_tell(fd, MYF(0));

    if (read == 0)
    {
      fprintf(stderr, "total_read: 0x%08llx chksum_pos: 0x%08llx, real_pos: 0x%08llx\n",
              total_read, chksum_pos, real_pos);
      die("chksum_read encountered an unexpected EOF");
    }

    if (read == MY_FILE_ERROR)
    {
      fprintf(stderr, "total_read: 0x%08llx chksum_pos: 0x%08llx, real_pos: 0x%08llx\n",
              total_read, chksum_pos, real_pos);
      die("chksum_read returned an error while reading from a file");
    }

    total_read+= read;

    fprintf(stderr, "0x%08llx of 0x%08llx\r", total_read, total_written);

    if (chksum_pos != total_read || chksum_pos < real_pos/2 || chksum_pos > real_pos)
    {
      fprintf(stderr, "total_read: 0x%08llx chksum_pos: 0x%08llx, real_pos: 0x%08llx\n",
              total_read, chksum_pos, real_pos);
      die("chksum I/O position has wrapped due to an improper cast/sign extension");
    }
  }

  (void) my_close(fd, MYF(0));

  fputs("Huge file checksumming tests passed.\n", stderr);
}

int main(int argc __attribute__((unused)), char** argv)
{
  MY_INIT(argv[0]);

  test_chksum_huge_files();
  return 0;
}

#endif
