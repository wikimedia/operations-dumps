#ifndef _MWBZUTILS_H
#define _MWBZUTILS_H

#include "bzlib_private.h"
int BZ_API(BZ2_bzDecompress_mine) ( bz_stream *strm );

typedef struct {
  int page_id; /* first id in the block */
  int bits_shifted; /* block is right shifted this many bits */
  off_t position; /* position in file of block */
} page_info_t;

#define BUFINSIZE 5000

/*
  keeps all information about a bzipped file
  plus input/output buffers for decompression
*/
typedef struct {
  unsigned char bufin[BUFINSIZE];   /* compressed data read from file */
  unsigned char *bufout;            /* uncompressed data, must be allocated by caller */
  unsigned char marker_buffer[7];    /* data to test for bz2 block marker */
  unsigned char header_buffer[4];    /* first 4 bytes of file (bzip2 header) */

  int bufin_size;                    /* size of input buffer for compressed data */
  int bufout_size;                   /* size of output buffer for decompressed data, may vary at each call */

  int initialized;                  /* whether bz2file has been initialized (header processed, seek to 
				       some bz2 block in the file and input buffer filled) */
  off_t block_start;                   /* position of bz2 block in file from which we started to read (we
                                       read a sequence of bz2 blocks from a given position, this is 
                                       the offset to the first one) */

  bz_stream strm;                   /* stream structure for libbz2 */
  unsigned char overflow;           /* since decompressed bytes may not be bit aligned, we keep the last byte
				       read around so we can grab the lower end bits off the end for
				       sticking in front of the next pile of compressed bytes we read */

  int bits_shifted;                  /* number of bits that the compressed data has been right shifted 
				       in the file (if the number is 0, the block marker and subsequent
				       data is byte-aligned) */
  unsigned char **marker;           /* bzip2 start of block marker, plus bit-shifted versions of it for
				       locating the marker in a stream of compressed data */
  unsigned char **footer;           /* bzip2 end of stream footer, plus bit-shifted versions of it for
				       locating the footer in a stream of compressed data */

  off_t position;                     /* current offset into file from start of file */

  int bytes_read;                    /* number of bytes of compressed data read from file (per read) */
  int bytes_written;                 /* number of bytes of decompressed data written into output buffer (per decompress) */
  int eof;                          /* nonzero if eof reached */
  off_t file_size;                     /* length of file, so we don't search past it for blocks */
} bz_info_t;

#define MASKLEFT 0
#define MASKRIGHT 1

/* 
   this output buffer is used to collect decompressed output.  
   this is not a circular buffer; when it is full the user is 
   responsible for emptying it completely or partially and moving 
   to the beginning any unused bytes. 
   
*/
typedef struct {
  unsigned char *buffer;          /* output storage, allocated by the caller */
  unsigned char *next_to_read;    /* pointer to the next byte in the buffer with data to be read */
  unsigned char *next_to_fill;    /* pointer to the next byte in the buffer which is empty and can receive data */
  int bytes_avail;                /* number of bytes available for reading */
  unsigned char *end;             /* points to byte after end of buffer */
} buf_info_t;

/* 
   used for each iteration of narrowing down the location in a bzipped2 file of
   a desired pageid, by finding first compressed block after a guessed  
   position and checking the first pageid (if any) contained in it.  
*/
typedef struct {
  off_t left_end;       /* left end of interval to search (bytes from start of file) */
  off_t right_end;      /* right end of interval to search */
  int value_wanted;   /* pageid desired */
  int last_value;     /* pageid we found in last iteration */
  off_t last_position;  /* position in file for last iteration */
} iter_info_t;

int bit_mask(int numbits, int end);

void shift_bytes_left(unsigned char *buffer, int buflen, int numbits);

void shift_bytes_right(unsigned char *buffer, int buflen, int numbits);

unsigned char ** init_marker();

int bytes_compare(unsigned char *buff1, unsigned char *buff2, int numbytes, int bitsrightshifted);

int check_buffer_for_bz2_block_marker(bz_info_t *bfile);

#define FORWARD 1
#define BACKWARD 2

int find_next_bz2_block_marker(int fin, bz_info_t *bfile, int direction);

int init_decompress(bz_info_t *bfile);

int decompress_header(int fin, bz_info_t *bfile);

int setup_first_buffer_to_decompress(int fin, bz_info_t *bfile);

int fill_buffer_to_decompress(int fin, bz_info_t *bfile, int ret);

buf_info_t *init_buffer(int size);

void free_buffer(buf_info_t *b);

int buffer_is_empty(buf_info_t *b);

int buffer_is_full(buf_info_t *b);

off_t get_file_size(int fin);

int init_bz2_file(bz_info_t *bfile, int fin, int direction);

int get_and_decompress_data(bz_info_t *bfile, int fin, unsigned char *bufferout, int bufout_size, int direction);

int get_buffer_of_uncompressed_data(buf_info_t *b, int fin, bz_info_t *bfile, int direction);

void dump_buf_info(buf_info_t *b);

int  move_bytes_to_buffer_start(buf_info_t *b, unsigned char *fromwhere, int maxbytes);

unsigned char ** init_footer();

int read_footer(unsigned char *buffer, int fin);

int check_file_for_footer(int fin, bz_info_t *bfile);

void clear_buffer(unsigned char *buf, int length);

off_t find_first_bz2_block_from_offset(bz_info_t *bfile, int fin, off_t position, int direction);

#endif
