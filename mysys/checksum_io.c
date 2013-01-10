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

typedef my_off_t block_num_t;
typedef uint16 block_offset_t;

/* Get the (zero-indexed) block number counted from the beginning of the file */
static block_num_t get_block_num(my_off_t offset)
{
  return offset / IOCACHE_FITTED_BLOCK_SIZE;
}

/* Get the byte offset from the beginning of the data section for the block we are in */
static block_offset_t get_block_offset(my_off_t offset)
{
  return offset % IOCACHE_FITTED_BLOCK_SIZE;
}

/* Get the location of the start of our block */
static my_off_t get_block_loc(block_num_t index)
{
  return index * IOCACHE_BLOCK_SIZE;
}

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
static my_off_t chksum_pos_ftor(my_off_t pos)
{
  block_offset_t block_offset= get_block_offset(pos);
  block_num_t block_num= get_block_num(pos);

  /* We do the last bit to add in the header size. */
  my_off_t real_pos= (my_off_t) block_num * IOCACHE_BLOCK_SIZE
      + block_offset + (IOCACHE_BLOCK_HEADER_SIZE * (block_offset != 0));

  return real_pos;
}

/*
  Convert real addressing to the fake address that MySQL sees

  @param[in] pos The real address

  @return the corresponding fake address
*/
static my_off_t chksum_pos_rtof(my_off_t pos)
{
  block_offset_t block_offset= pos % IOCACHE_BLOCK_SIZE;
  block_num_t block_num= pos / IOCACHE_BLOCK_SIZE;
  my_off_t fake_pos;

  /*
    if we are given a real pos inside a block header, align to the
    beginning of the block data
  */
  if (block_offset == 0)
    block_offset+= IOCACHE_BLOCK_HEADER_SIZE;

  /* We do the last bit to account for the header size in the last block */
  fake_pos= (my_off_t) block_num * IOCACHE_FITTED_BLOCK_SIZE + block_offset
    - IOCACHE_BLOCK_HEADER_SIZE;

  return fake_pos;
}


/*
  Copy a section of our Buffer into the given block

  This function copies data into the block and recalculates
  the requisite chksum and len.

  This function must work correctly when start is equal to
  IOCACHE_FITTED_BLOCK_SIZE. It must also work correctly when count is 0.
  This is useful for extending the last block in the file.

  @param[in,out]  block   The chksum_block we will be writing our data into
  @param[in]      Buffer  The data we will be copying into the block
  @param[in]      start   The offset from the beginning of the block to which
                          we should begin copying the data
  @param[in]      count   The total number of bytes that are left copied from
                          Buffer

  @return length written
*/
static block_offset_t insert_into_block(CHKSUM_BLOCK *block,  const uchar *Buffer,
                                        block_offset_t start, size_t count)
{
  DBUG_ASSERT(start <= IOCACHE_FITTED_BLOCK_SIZE);

  /*
    If we are writing past the end of the block, we must zero the memory in
    between, or we risk leaking some of our memory contents.
  */
  if (start > block->len)
  {
      memset(block->data + block->len, 0, start - block->len);
  }

  block_offset_t to_write= min(count, IOCACHE_FITTED_BLOCK_SIZE - start);
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

  @return TRUE on success, FALSE on my_pwrite failure
*/
static my_bool chksum_write_block(File file, CHKSUM_BLOCK *block, block_num_t block_num)
{
  my_off_t block_loc= get_block_loc(block_num);

  DBUG_ASSERT(block->len <= IOCACHE_FITTED_BLOCK_SIZE);
  size_t to_write= block->len + IOCACHE_BLOCK_HEADER_SIZE;
  size_t res= my_pwrite(file, (void *) block, to_write, block_loc, MYF(0));

  if (res != to_write)
  {
    fprintf(stderr,
            "chksum_write_block failed due to my_pwrite(fd=%d) returning %llu with errno=%d, "
            "while trying to write block %llu, at block_loc %llu\n",
            file, (ulonglong) res, my_errno, block_num, block_loc);
    return FALSE;
  }
  return TRUE;
}

/*
  Retrieve the block at the given index, validating its checksum.
  If the checksum check fails, the server is aborted.

  @param[in]    file      The file we should read from
  @param[out]   block     The block we will read our data into
  @param[in]    block_num The index (zero-indexed) of the block we want

  @return
    @retval SUCCESS TRUE
    @retval ERROR   FALSE
*/
static my_bool get_block_at_index(File file, CHKSUM_BLOCK *block, block_num_t block_num)
{
  my_off_t block_loc= get_block_loc(block_num);

  size_t bytes_read= 0;
  while (bytes_read < IOCACHE_BLOCK_SIZE)
  {
    size_t res= my_pread(file, (uchar *) block + bytes_read,
                         IOCACHE_BLOCK_SIZE - bytes_read,
                         block_loc + bytes_read, MYF(0));
    if (res == 0)
    {
      /* we reached the end of file */
      break;
    }
    else if (res == MY_FILE_ERROR)
    {
      fprintf(stderr,
              "get_block_at_index failed in my_pread(fd=%d), errno=%d, "
              "while trying to read block %llu, at block_loc %llu\n",
              file, my_errno, block_num, block_loc);
      return FALSE;
    }
    else
    {
      bytes_read+= res;
    }
  }

  if (bytes_read == 0)
  {
    block->len= 0;
  }
  else if (bytes_read >= IOCACHE_BLOCK_HEADER_SIZE)
  {
    block->len= bytes_read - IOCACHE_BLOCK_HEADER_SIZE;
    ha_checksum chksum= my_checksum(0L, block->data, block->len);
    if (chksum != block->chksum)
    {
      goto error;
    }
  }
  else /* bytes_read < IOCACHE_BLOCK_HEADER_SIZE */
  {
    goto error;
  }

  return TRUE;

error:
  fprintf(stderr,
          "Block corruption found in IO Cache: checksums do not match\n"
          "Aborting server...\n");
  exit(1);
}

/*
  Grows the file to the requested (virtual) size by padding it with NUL
  characters and writing proper checksums for these blocks.

  If the file does not need to be extended, nothing is done and the function
  returns successfully.

  @param[in]  file       File descriptor
  @param[in]  to_size    Virtual size that the file needs to be extended to

  @return
    @retval ERROR    FALSE
    @retval SUCCESS  TRUE
*/
static my_bool chksum_grow_file(File file, my_off_t to_size)
{
  /* Get current file size */
  my_off_t pos, file_size;
  if (MY_FILEPOS_ERROR == (pos= my_tell(file, MYF(0))) ||
      MY_FILEPOS_ERROR == (file_size= my_seek(file, 0, MY_SEEK_END, MYF(0))) ||
      MY_FILEPOS_ERROR == my_seek(file, pos, MY_SEEK_SET, MYF(0)))
    return FALSE;

  /* Resize the file if needed */
  my_off_t offset= file_size;
  while (offset < to_size)
  {
    CHKSUM_BLOCK block;
    block_num_t block_num = get_block_num(offset);
    block_offset_t block_offset= get_block_offset(offset);

    my_bool got_block= get_block_at_index(file, &block, block_num);
    if (!got_block)
      return FALSE;

    block_offset_t amount_to_clear= min(IOCACHE_FITTED_BLOCK_SIZE - block_offset,
                                        to_size - offset);
    (void) insert_into_block(&block, NULL, block_offset + amount_to_clear, 0);

    my_bool write_successful= chksum_write_block(file, &block, block_num);
    if (!write_successful)
      return FALSE;

    offset+= amount_to_clear;
  }
  return TRUE;
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
*/
static block_offset_t read_from_block(CHKSUM_BLOCK *block, uchar *Buffer,
                                      block_offset_t start, size_t count)
{
  DBUG_ASSERT(start <= IOCACHE_FITTED_BLOCK_SIZE);

  if (start >= block->len)
  {
    return 0;
  }

  block_offset_t to_read= min(count, block->len - start);
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
  my_off_t real_pos;
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
  DBUG_PRINT("chksum", ("fd: %d, pos: %llu, whence: %d", (int) fd, pos, (int) whence));

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
    retval= my_seek(fd, 0, MY_SEEK_END, MYF(0));
    if (retval == MY_FILEPOS_ERROR)
      return retval;
    /* would probably work better to just use my_stat() */
    new_pos= my_tell(fd, MYF(0));
    if (new_pos == MY_FILEPOS_ERROR)
      return new_pos;

    /* strange handling because we are counting chksum headers backwards */
    new_pos= new_pos + pos + get_block_num(pos) * IOCACHE_BLOCK_HEADER_SIZE;
    break;
  default:
    DBUG_PRINT("error", ("chksum_seek didn't recieve a recognized `whence`"));
    DBUG_RETURN(retval);
  }

  retval= my_seek(fd, new_pos, MY_SEEK_SET, MyFlags);
  if (retval != MY_FILEPOS_ERROR)
    retval= chksum_pos_rtof(retval);
  DBUG_RETURN(retval);
}


/* Used only by the test suite */
static void test_corrupt_checksum_block(File Filedes)
{
  uchar fake_buffer[IOCACHE_BLOCK_SIZE * 4];
  memset(fake_buffer, 0, IOCACHE_BLOCK_SIZE * 4);
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

  DBUG_ENTER("chksum_pread");
  DBUG_PRINT("chksum", ("Filedes %d, Buffer %p, Count %llu",
                        (int) Filedes, (void *) Buffer, (ulonglong) Count));

  DBUG_ASSERT(IOCACHE_BLOCK_SIZE == sizeof(CHKSUM_BLOCK)
              - sizeof(block.len));

  DBUG_EXECUTE_IF("chksum_block_corrupted",
                  test_corrupt_checksum_block(Filedes););

  /* doesn't count block metadata */
  size_t bytes_read= 0;
  size_t cnt= Count;

  while (cnt != 0)
  {
    block_num_t block_num= get_block_num(offset);
    block_offset_t block_offset= get_block_offset(offset);

    my_bool got_block= get_block_at_index(Filedes, &block, block_num);
    if (!got_block)
      DBUG_RETURN(MY_FILE_ERROR);

    block_offset_t read_size= read_from_block(&block, Buffer, block_offset, cnt);
    bytes_read+= read_size;

    /* Check for the end of file */
    if (read_size == 0 || block.len < IOCACHE_FITTED_BLOCK_SIZE)
      break;

    DBUG_ASSERT(0 < read_size && read_size <= cnt);

    cnt-= read_size;
    Buffer+= read_size;
    offset+= read_size;
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
    @retval ERROR    MY_FILE_ERROR
    @retval SUCCESS  0  if flag has bits MY_NABP or MY_FNABP set
    @retval SUCCESS  N  number of bytes read.
*/

size_t chksum_read(File Filedes, uchar *Buffer, size_t Count, myf MyFlags)
{
  my_off_t offset;
  size_t bytes_read;

  DBUG_ENTER("chksum_read");
  DBUG_PRINT("chksum", ("Filedes %d, Buffer %p, Count %llu",
                        (int) Filedes, (void *) Buffer, (ulonglong) Count));

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
    @retval ERROR     MY_FILE_ERROR
    @retval SUCCESS   Number of bytes read
*/
size_t chksum_pwrite(File Filedes, const uchar *Buffer, size_t Count,
                     my_off_t offset, myf MyFlags)
{
  /* containing block for the start of our read */
  CHKSUM_BLOCK block;

  DBUG_ENTER("chksum_pwrite");
  DBUG_PRINT("chksum", ("Filedes %d, Buffer %p, Count %llu, offset %llu",
                        (int) Filedes, (void *) Buffer, (ulonglong) Count, offset));

  /* Determine if we are writing past the end of file and there are blocks
   * that need to be adjusted, so that the CRC matches after inserting zeroes
   * between the current end of file and the data we are about to write.
   */
  if (!chksum_grow_file(Filedes, offset))
  {
    return MY_FILE_ERROR;
  }

  /* doesn't count block metadata */
  size_t total_written= 0;
  size_t cnt= Count;

  while (cnt != 0)
  {
    block_num_t block_num= get_block_num(offset);
    block_offset_t block_offset= get_block_offset(offset);

    my_bool got_block= get_block_at_index(Filedes, &block, block_num);
    if (!got_block)
      DBUG_RETURN(MY_FILE_ERROR);

    block_offset_t written= insert_into_block(&block, Buffer, block_offset, cnt);
    my_bool write_successful= chksum_write_block(Filedes, &block, block_num);

    if (!write_successful)
      DBUG_RETURN(MY_FILE_ERROR);

    DBUG_ASSERT(0 < written && written <= cnt);
    cnt-= written;
    Buffer+= written;
    total_written+= written;
    offset+= written;
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
    @retval ERROR    MY_FILE_ERROR
    @retval SUCCESS  0  if flag has bits MY_NABP or MY_FNABP set
    @retval SUCCESS  N  number of bytes written.
*/

size_t chksum_write(File Filedes, const uchar *Buffer, size_t Count, myf MyFlags)
{
  my_off_t loc;
  size_t res;
  my_off_t seek_res;

  DBUG_ENTER("chksum_write");
  DBUG_PRINT("chksum", ("Filedes %d, Buffer %p, Count %llu",
                        (int) Filedes,(void *) Buffer, (ulonglong) Count));
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
      DBUG_RETURN(MY_FILE_ERROR);
  }

  if (MyFlags & (MY_NABP | MY_FNABP))
    DBUG_RETURN(0);                             /* Want only errors */
  DBUG_RETURN(res);
}

/**********************************************************************
 Testing of checksum_io
**********************************************************************/

#ifdef MAIN

static const uchar *test_string= (const uchar *) "Li Europan lingues es membres del sam familie.";

static void die(const char *fmt, ...)
{
  va_list va_args;
  va_start(va_args,fmt);
  fprintf(stderr,"Error:");
  vfprintf(stderr, fmt, va_args);
  fprintf(stderr,", errno=%d\n", errno);
  exit(1);
}

static File open_temp_file(const char *name_prefix)
{
  char fname[FN_REFLEN];
  File fd= create_temp_file(fname, NULL, name_prefix, O_CREAT | O_RDWR,  MYF(0));

  if (fd < 0)
    die("failed to create a temporary file");

  (void) my_delete(fname, MYF(0));
  return fd;
}

static my_bool check_mem_eq(const void *buf, char cmp, size_t count)
{
  const char *cbuf = (const char *) buf;
  for (size_t i = 0; i < count; ++i)
  {
    if (cbuf[i] != cmp)
      return FALSE;
  }
  return TRUE;
}

static void test_pos_translation()
{
  fputs("Testing chksum_pos_{ftor,rtof} position translation...\n", stderr);

  my_off_t offsets[]= { 904435, 8192, 24576, 24592 };

  for (size_t i = 0; i < sizeof offsets / sizeof offsets[0]; ++i)
  {
    my_off_t off= offsets[i];
    /*
      Note that the opposite order of conversions may not always return the
      same offset: chksum_pos_rtof aligns the real positions pointing inside of
      the auxiliary data-structures to the next fake position available.

      Here we are guaranteed to always get a real position that points
      somewhere in the real data.
    */
    if (off != chksum_pos_rtof(chksum_pos_ftor(off)))
      die("bug in chksum_pos_ftor/chksum_pos_rtof position conversion");
  }
}

static void test_insert_into_block_clears_empty_ranges()
{
  /*
    Plan: produce the block with the following contents layout:
      0..len-1:        test_string,
      len..3*len-1:    zeroes,
      3*len..4*len-1:  test_string,
    where len is strlen(test_string), and ensure that the operations that
    produce/read block work as expected.
  */
  fputs("Testing insert_into_block behavior on inserts past the end of the block...\n", stderr);

  uchar block_data[IOCACHE_FITTED_BLOCK_SIZE];

  CHKSUM_BLOCK block;
  const char filler= 0xa6;
  memset(&block, filler, sizeof block);
  block.len= 0;

  const size_t len= strlen((const char *) test_string);

  block_offset_t inserted, read_size;

  inserted= insert_into_block(&block, test_string, 0, len);
  if (inserted != len)
    die("insert_into_block failed to insert data into a block");

  inserted= insert_into_block(&block, NULL, 2 * len, 0);

  memset(block_data, filler, sizeof block_data);
  read_size= read_from_block(&block, block_data, len, len);

  if (inserted != 0 ||
      read_size != len ||
      !check_mem_eq(block_data, 0, len) ||
      !check_mem_eq(block_data + len, filler, sizeof block_data - len))
    die("insert_into_block failed to pad data in the block correctly");

  /*
    Insert the test_string contents into the block some distance from the end
    of the previously inserted data. insert_into_block must handle that
    correctly and zero the hole.
  */
  inserted= insert_into_block(&block, test_string, 3 * len, len);
  if (inserted != len)
    die("insert_into_block failed to insert data into a block");

  memset(block_data, filler, sizeof block_data);
  read_size= read_from_block(&block, block_data, 0, sizeof block_data);

  if (!check_mem_eq(block_data + 2 * len, 0, len))
    die("insert_into_block failed to zero the hole when inserting data past the end of the block");

  if (read_size != 4 * len ||
      0 != memcmp(test_string, block_data, len) ||
      0 != memcmp(test_string, block_data + 3 * len, len) ||
      !check_mem_eq(block_data + 4 * len, filler, sizeof block_data - 4 * len))
    die("insert_into_block failed to insert or read_from_block failed to read the data correctly");
}

static void test_insert_into_block_on_empty_inserts()
{
  fputs("Testing insert_into_block behavior on empty inserts...\n", stderr);

  CHKSUM_BLOCK block;
  block.len= 0;

  uchar buf[sizeof block.data];
  const size_t len= sizeof buf;
  memset(buf, 0, len);

  block_offset_t inserted;

  inserted= insert_into_block(&block, buf, 0, len / 2);
  if (inserted != len / 2 || block.len != len / 2)
    goto error;

  inserted= insert_into_block(&block, buf, len / 2, 0);
  if (inserted != 0 || block.len != len / 2)
    goto error;

  inserted= insert_into_block(&block, buf, len / 4, 0);
  if (inserted != 0 || block.len != len / 2)
    goto error;

  inserted= insert_into_block(&block, buf, len / 2, len);
  if (inserted != len - len / 2 || block.len != len)
    goto error;

  inserted= insert_into_block(&block, buf, block.len, len);
  if (inserted != 0 || block.len != len)
    goto error;
  return;

error:
    die("insert_into_block failed to insert data into a block");
}

static void test_read_from_block_returns_data_if_available()
{
  fputs("Testing read_from_block behavior on read request for more data than is available...\n", stderr);

  CHKSUM_BLOCK block;
  block.len= 0;

  const size_t len= strlen((const char *) test_string);

  block_offset_t inserted= insert_into_block(&block, test_string, 0, len);
  if (inserted != len)
    die("insert_into_block failed to insert data into a block");

  uchar *block_data= alloca(2 * len);
  block_offset_t read_size= read_from_block(&block, block_data, 0, 2 * len);

  if (read_size != len || 0 != memcmp(test_string, block_data, len))
    die("read_from_block failed to return all the available data from the block");
}

static void test_chksum_pwrite_past_the_end_of_the_file()
{
  fputs("Testing chksum_pwrite correctly extends the file if the offset is past its end...\n", stderr);

  File fd= open_temp_file("chksum_pwrite_past_end.");
  const size_t len= strlen((const char*) test_string);

  size_t res= chksum_write(fd, test_string, len, MYF(0));
  if (res != len || chksum_tell(fd, MYF(0)) != len)
    die("chksum_write returned an error while writing to a file");

  const my_off_t big_offset = 0x123456789ULL;
  res= chksum_pwrite(fd, test_string, len, big_offset, MYF(0));

  if (res != len)
    die("chksum_pwrite returned an error while writing to a file");

  my_off_t file_size= chksum_seek(fd, 0, SEEK_END, MYF(0));
  if (file_size != big_offset + len)
    die("chksum_pwrite didn't extend the file correctly");

  uchar buf[4096];
  chksum_seek(fd, 0, SEEK_SET, MYF(0));
  my_off_t bytes_read= chksum_read(fd, buf, len, MYF(0));

  if (bytes_read != len || 0 != memcmp(test_string, buf, len))
    die("chksum_write wrote incorrect data");

  while (bytes_read < big_offset)
  {
    size_t to_read= min(sizeof buf, big_offset - bytes_read);
    res= chksum_read(fd, buf, to_read, MYF(0));
    if (res != to_read)
      die("chksum_pwrite did not extend the file properly");

    if (!check_mem_eq(buf, 0, to_read))
      die("chksum_pwrite did not extend the file with zeros");

    bytes_read+= res;
  }

  res= chksum_read(fd, buf, len, MYF(0));

  if (res != len || 0 != memcmp(test_string, buf, len))
    die("chksum_pwrite wrote incorrect data");
}

static void test_chksum_huge_files()
{
  fputs("Testing huge checksummed file writing...\n", stderr);

  /* this would overflow ints if they are used where they should not */
  const my_off_t file_size= 0x123456789ULL;

  File fd= open_temp_file("chksum_write_huge_test.");

  uchar buf[4096];
  memset(buf, 0xa6, 4096);

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

    /*
      chksum_pos can never be greater than the real_pos, since checksumming
      has some overhead.  However this overhead is supposed to be small, so
      chksum_pos cannot be much smaller than the real_pos.
    */
    if (chksum_pos != total_written || chksum_pos < real_pos / 2 || chksum_pos > real_pos)
    {
      fprintf(stderr, "total_written: 0x%08llx chksum_pos: 0x%08llx, real_pos: 0x%08llx\n",
              total_written, chksum_pos, real_pos);
      die("chksum I/O position has wrapped due to an improper cast/sign extension");
    }
  }
  chksum_seek(fd, 0, MY_SEEK_SET, MYF(0));

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

    if (chksum_pos != total_read || chksum_pos < real_pos / 2 || chksum_pos > real_pos)
    {
      fprintf(stderr, "total_read: 0x%08llx chksum_pos: 0x%08llx, real_pos: 0x%08llx\n",
              total_read, chksum_pos, real_pos);
      die("chksum I/O position has wrapped due to an improper cast/sign extension");
    }
  }

  (void) my_close(fd, MYF(0));

  fputs("Huge file checksumming tests passed.\n", stderr);
}

int main(int argc __attribute__((unused)), char **argv)
{
  MY_INIT(argv[0]);

  test_pos_translation();
  test_insert_into_block_clears_empty_ranges();
  test_insert_into_block_on_empty_inserts();
  test_read_from_block_returns_data_if_available();
  test_chksum_pwrite_past_the_end_of_the_file();
  test_chksum_huge_files();
  return 0;
}

#endif
