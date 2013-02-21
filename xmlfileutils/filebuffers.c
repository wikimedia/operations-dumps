#include <stdio.h>
#include <getopt.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/types.h>
#include <regex.h>
#include <ctype.h>
#include <inttypes.h>
#include <time.h>
#include <bzlib.h>
#include <zlib.h>
#include <stdarg.h>

#include "mwxml2sql.h"

/*
  args:
     contents             - data to compress
     compressed_length    - where length of compressed data will be stored
     gz_buf               - where compressed data will be stored
     gz_buf_length        - size of buffer for compressed data

  returns a pointer to the compressed data, or NULL on error

  note that gz_buf must be at least 1.015* size of input plus 5 
*/
char *gzipit(char *contents, int *compressed_length, char *gz_buf, int gz_buf_length) {
  z_stream stream;

  stream.next_in = (Bytef *)contents;
  stream.avail_in = strlen(contents);
  stream.zalloc = Z_NULL;
  stream.zfree = Z_NULL;
  stream.opaque = Z_NULL;

  stream.avail_out = gz_buf_length;
  stream.next_out = (Bytef *)gz_buf;

  if (deflateInit(&stream, Z_DEFAULT_COMPRESSION) != Z_OK) {
    fprintf(stderr,"Failed to set up for gzip  compression of data\n");
    return(NULL);
  }
  deflate(&stream, Z_FINISH);
  deflateEnd(&stream);
  *compressed_length = (char *)(stream.next_out) - gz_buf;
  return(gz_buf);
}

/*
  args:
    b      z2 buffer

  returns 1if the buffer is full, 0 otherwise
*/
int isfull(bz2buffer_t *b) {
  if (b->nextin == sizeof(b->buf)) return(1);
  else return(0);
}

/*
  args:
    b      bz2 buffer
    fd     pointer to file structure for open bz2 file

    returns: 1 on success, 0 otherwise (e.g. eof encountered)
*/  
int fill_buffer(bz2buffer_t *b, BZFILE *fd) {
  int result;

  if (isfull(b)) return(1);
  result = BZ2_bzread(fd, b->buf + b->nextin, sizeof(b->buf) - b->nextin);
  if (result) {
    b->nextin += result;
    b->bytes_avail += result;
    return(1);
  }
  else return (0);
}

/* 
  args:
    b      bz2 buffer

   check available output bytes for '\n' 

   return index of newline in string (relative to 
   nextout)
   (which may be '\0' or past the allocated buffer length)
   or -1 if not found
*/
int has_newline(bz2buffer_t *b) {
  int ind = 0;
  while (ind < b->bytes_avail) {
    if (b->buf[b->nextout+ind] == '\n') return(ind+1);
    ind++;
  }
  return(-1);
}

/*
  args:
    b      bz2 buffer

    writes contents of the bz2 buffer structure to
    stderr, for debugging use

*/
void dump_bz2buffer(bz2buffer_t *b) {
  int ind;

  if (!b) {
    fprintf(stderr,"bz2 buffer is NULL\n");
    return;
  }
  fprintf(stderr,"nextin: %d, nextout: %d, bytes_avail: %d\n", b->nextin, b->nextout, b->bytes_avail);
  fprintf(stderr,"buffer avail bytes: >>");
  ind = b->nextout;
  while (ind < b->nextin) {
    fprintf(stderr,"%c",b->buf[ind]);
    ind++;
  }
  fprintf(stderr,"<<\n");
  return;
}

/*
  args:
    fd        structure for bz2 input file
    b         bz2 buffer
    out       holder where data read will be copied
    out_size  size of holder
    
  returns:
    pointer to the output buffer if any data was read and copied
    NULL otherwise

  this function will read one line of output from file and copy it
  into out, at most out_size -1 bytes are copied, a '\0' will be placed at the end,
  if no input is copied the holder will contain the empty string
*/
char *bz2gets(BZFILE *fd, bz2buffer_t *b, char *out, int out_size) {
  int newline_ind = -1;
  int out_ind = 0;
  int out_space_remaining = out_size -1;

  out[0]='\0';
  if (!b->bytes_avail) fill_buffer(b, fd);
  if (!b->bytes_avail) {
    return(0);
  }

  while (((newline_ind = has_newline(b)) == -1) && (out_space_remaining > b->bytes_avail)) {
    strncpy(out+out_ind, b->buf + b->nextout, b->bytes_avail);
    out_ind += b->bytes_avail;
    out[out_ind] = '\0';
    out_space_remaining -= b->bytes_avail;
    b->nextout = b->nextin = b->bytes_avail = 0;
    fill_buffer(b, fd);
    if (!b->bytes_avail) {
      out[out_ind] = '\0';
      if (out_ind) return(out);
      else return(NULL);
    }
  }
  if (out_space_remaining) {
    if (newline_ind >=0 && newline_ind < out_space_remaining) {
      strncpy(out+out_ind, b->buf + b->nextout, newline_ind);
      out_ind += newline_ind;
      out[out_ind] = '\0';
      b->nextout += newline_ind;
      b->bytes_avail -= (newline_ind);
    }
    else {
      strncpy(out+out_ind, b->buf + b->nextout, out_space_remaining);
      out_ind+= out_space_remaining;
      out[out_ind] = '\0';
      b->nextout += out_space_remaining;
      b->bytes_avail -= out_space_remaining;
    }
    /* if the buffer is empty set things up correctly for that case */
    if (b->nextout == sizeof(b->buf) && !b->bytes_avail) {
      b->nextout = 0;
      b->nextin = 0;
    }
  }
  out[out_ind] = '\0';
  if (!out_ind) return(NULL);
  else return(out);
}

/*
  args:
    f         structure for input file
    buf       holder where data read will be copied
    length    size of holder
    
  returns:
    pointer to the output buffer if any data was read and copied
    NULL otherwise

  this function will read one line of output from file and copy it
  into buf, at most length -1 bytes are copied, a '\0' will be placed at the end,
  if no input is copied the holder will contain the empty string

  this function handles input files of type plain text,  gz or bzip2
   compressed.
*/
char *get_line2buffer(input_file_t *f, char *buf, int length) {
  buf[0]='\0';
  if (f->filetype == BZCOMPRESSED)
    return(bz2gets(f->bz2fd, f->xmlb, buf, length));
  if (f->filetype == GZCOMPRESSED)
    return(gzgets(f->gzfd, buf, length));
  else
    return(fgets(buf, length, f->fd));
}

/*
  args:
    f         structure for input file
    
  returns:
    pointer to the file cntent buffer if any data was read and copied
    NULL otherwise

  this function will read one line from file and copy it into the file
  content buffer, a '\0' will be placed at the end
  if f->leftover is non-empty, those bytes will be copied into the
  beginning of the file content buffer and the data read from the file
  will be stored immediately after it.
  if no data is read, the file content buffer will contain the empty
  string, or if f->leftover had content, a copy of that data null-
  terminated

  this function handles input files of type plain text,  gz or bzip2
   compressed.
*/
char *get_line(input_file_t *f) {
  char *start = NULL;
  int length = 0;

  start = f->in_buf->content;
  length = f->in_buf->length;
  if (f->leftover[0]) {
    strcpy(f->in_buf->content, f->leftover);
    start += strlen(f->leftover);
    length -= strlen(f->leftover);
    f->leftover[0] = '\0';
  }
  else {
    f->in_buf->content[0] = '\0';
  }
  if (f->filetype == BZCOMPRESSED)
    return(bz2gets(f->bz2fd, f->xmlb, start, length)?f->in_buf->content:NULL);
  else if (f->filetype == GZCOMPRESSED)
    return(gzgets(f->gzfd, start, length)?f->in_buf->content:NULL);
  else
    return(fgets(start, length, f->fd)?f->in_buf->content:NULL);
}

/* 
  args:
     f      structure for output file
     line   null terminates string to write to file

  returns:
     0 on error, nonzero otherwise

  this function will write the given line of output
  to the spcified file

   expects a trailing newline if you want one in there :-P 
*/
int put_line(output_file_t *f, char *line) {
  if (f->filetype == BZCOMPRESSED)
    return(BZ2_bzwrite(f->bz2fd, line, strlen(line)));
  else if (f->filetype == GZCOMPRESSED)
    return(gzputs(f->gzfd, line));
  else
    return(fputs(line, f->fd));
}

/*
  args:
     f      list of structures for output files
     line   null terminates string to write to files

  returns:
     0 if any write encounters an error, nonzero otherwise

  this function will write the given line of output
  to each file in the list
*/
int put_line_all(output_file_t *f, char *line) {
  int result = 0;

  while (f) {
    result = put_line(f, line);
    if (!result) return(0);
    f = f->next;
  }
  return(0);
}

/*
  args:
     b    file input buffer

  this function frees a file input buffer
*/
void free_input_buffer(string_t *b) {
  if (b) {
    if (b->content) free(b->content);
    free(b);
  }
  return;
}

/*
  returns:
     allocated and initialized file input buffer or
     NULL on error
*/
string_t *init_input_buffer() {
  string_t *b;

  b = (string_t *) malloc(sizeof(string_t));
  if (!b) {
    fprintf(stderr,"failed to get memory for input buffer\n");
    return(NULL);
  }
  b->length = TEXT_BUF_LEN;
  b->content = (char *)malloc(b->length);
  if (!b->content) {
    fprintf(stderr,"failed to get memory for input buffer\n");
    free_input_buffer(b);
    return(NULL);
  }
  return(b);
}

/*
  args:
     b   buffer for bz2 file reads

  this function frees a bz2 file input buffer
*/
void free_bz2buf(bz2buffer_t *b) {
  if (b) free(b);
  return;
}

/*
  returns:
     allocated and initialized bz2 file input buffer or
     NULL on error
*/
bz2buffer_t *init_bz2buf() {
  bz2buffer_t *b;

  b = (bz2buffer_t *) malloc(sizeof(bz2buffer_t));
  if (!b) {
    fprintf(stderr,"failed to get memory for bz2 input buffer\n");
    return(NULL);
  }
  b->nextin = b->nextout = b->bytes_avail = 0;
  return(b);
}

/*
  args:
    f    structure for output file

    this function frees a structure allocated
    for an input file
*/
void free_input_file(input_file_t *f) {
  if (f) {
    if (f->in_buf) free_input_buffer(f->in_buf);
    if (f->xmlb) free_bz2buf(f->xmlb);
    free(f);
  }
  return;
}

/*
  args:
    f    head of list of structures for output file

    this function frees a list of structures allocated
    for output files
*/
void free_output_file(output_file_t *f) {
  output_file_t *next;

  while (f) {
    next = f->next;
    if (f->filename) free(f->filename);
    free(f);
    f = next;
  }
  return;
}

/*
  args:
    filename     name of input file

  returns:
    allocated and filled in input file structure on success
    NULL on error

  this function handles gzipped, bz2 or plain text.files
  It expects the filename to end in .gz for gzipped, .bz2
  for bz2zipped and anything else for text files

  if no filename is supplied, the function will assume that
  reads come from stdin and will set things up accordingly
*/
input_file_t *init_input_file(char *filename) {
  input_file_t *inf;

  inf = (input_file_t *)malloc(sizeof(input_file_t));
  if (!inf) {
    fprintf(stderr,"failed to get memory for input file information\n");
    return(NULL);
  }
  inf->fd = NULL;
  inf->gzfd = NULL;
  inf->bz2fd = NULL;
  inf->xmlb = NULL;
  inf->in_buf = NULL;
  inf->filename = filename;
  if (filename == NULL) {
    inf->filetype = PLAINTEXT;
    inf->fd = stdin;
  }
  else if (!strcmp(filename+(strlen(filename) - 4), BZSUFFIX)) {
    inf->filetype = BZCOMPRESSED;
    inf->bz2fd = BZ2_bzopen(filename, "r");
    if (!inf->bz2fd) {
      fprintf(stderr,"failed to open bz2 file for read\n");
      free_input_file(inf);
      return(NULL);
    }
    inf->xmlb = init_bz2buf();
    if (!inf->xmlb) {
      fprintf(stderr,"failed to get memory for bz2 input buffer\n");
      free_input_file(inf);
      return(NULL);
    }
  }
  else if (!strcmp(filename+(strlen(filename) - 3), GZSUFFIX)) {
    inf->filetype = GZCOMPRESSED;
    inf->gzfd = gzopen(filename, "r");
    if (!inf->gzfd) {
      fprintf(stderr,"failed to open gz file for read");
      free_input_file(inf);
      return(NULL);
    }
  }
  else {
    inf->filetype = PLAINTEXT;
    inf->fd = fopen (filename, "r");
    if (!inf->fd) {
      fprintf(stderr,"failed to open file for read");
      free_input_file(inf);
      return(NULL);
    }
  }
  inf->in_buf = (string_t *) malloc(sizeof(string_t));
  if (!inf->in_buf) {
    fprintf(stderr,"failed to get memory for input buffer\n");
    free_input_file(inf);
    return(NULL);
  }
  inf->in_buf->length = TEXT_BUF_LEN;
  inf->in_buf->content = (char *)malloc(inf->in_buf->length);
  if (!inf->in_buf->content) {
    fprintf(stderr,"failed to get memory for input buffer\n");
    free_input_file(inf);
    return(NULL);
  }
  inf->leftover[0] = '\0';
  return(inf);
}

/*
  args:
    basename     name of output file without suffix (.gz, .bz2 etc)
    suffix       suffix of filename
    mwv          list of structures with information about the MediaWiki
                 versions for which sql output in these files will
                 be produced

  returns:
    allocated and filled in output file structure on success
    NULL on error

  this function handles gzipped, bz2 or plain text.files

  if no filename is supplied, the function will assume that
  writes go to stdout and will set things up accordingly
  this is likely not what you want unless you are dealing with only
  one write stream!
*/
output_file_t *init_output_file(char *basename, char *suffix, mw_version_t *mwv) {
  output_file_t *outf, *current, *head = NULL;
  mw_version_t *next = NULL;

  /* do this now for each mwv... */
  while (mwv) {
    next = mwv->next;

    outf = (output_file_t *)malloc(sizeof(output_file_t));
    if (!outf) {
      fprintf(stderr,"failed to get memory for output file information\n");
      return(NULL);
    }
    if (!head) head = outf; /* first time through */
    else current->next = outf; /* append to list */

    outf->fd = NULL;
    outf->gzfd = NULL;
    outf->bz2fd = NULL;
    outf->filename = NULL;
    outf->mwv = mwv;
    outf->next = NULL;

    mwv = next;

    if (basename == NULL) {
      outf->filetype = PLAINTEXT;
      outf->fd = stdin;
      continue;
    }

    /* "basename-" + version + suffix (if there is one) */
    outf->filename = (char *)malloc(strlen(basename) + (suffix?strlen(suffix):0) + strlen(outf->mwv->version) + 2);
    if (!outf->filename) {
      fprintf(stderr,"failed to get memory for output file information\n");
      free_output_file(head);
      return(NULL);
    }
    sprintf(outf->filename, "%s-%s%s", basename, outf->mwv->version, suffix?suffix:"0");
    if (!suffix) {
      outf->filetype = PLAINTEXT;
      outf->fd = fopen (outf->filename, "w");
      if (!outf->fd) {
	fprintf(stderr,"failed to open file for write");
	free_output_file(head);
	return(NULL);
      }
    }
    else if (!strcmp(suffix,BZSUFFIX)) {
      outf->filetype = BZCOMPRESSED;
      outf->bz2fd = BZ2_bzopen(outf->filename, "w");
      if (!outf->bz2fd) {
	fprintf(stderr,"failed to open bz2 file for write");
	free_output_file(head);
	return(NULL);
      }
    }
    else if (!strcmp(suffix, GZSUFFIX)) {
      outf->filetype = GZCOMPRESSED;
      outf->gzfd = gzopen(outf->filename, "w");
      if (!outf->gzfd) {
	fprintf(stderr,"failed to open gz file for write");
	free_output_file(head);
	return(NULL);
      }
    }
    else {
      outf->filetype = PLAINTEXT;
      outf->fd = fopen (outf->filename, "w");
      if (!outf->fd) {
	fprintf(stderr,"failed to open file for write");
	free_output_file(head);
	return(NULL);
      }
    }

    mwv = next;
    current = outf;
  }
  return(head);
}

/*
  args:
    f    structure for input file

    this function closes a file opened for
    input, whether gzipped, bz2 or plain text.
*/
void close_input_file(input_file_t *f) {
  if (f) {
    if (f->fd) 
      fclose(f->fd);
    else if (f->gzfd)
      gzclose(f->gzfd);
    else if (f->bz2fd)
      BZ2_bzclose(f->bz2fd);
  }
  return;
}

/*
  args:
    f    head of list of structures for output files

    this function closes files opened for
    output, whether gzipped, bz2 or plain text.
*/
void close_output_file(output_file_t *f) {
  output_file_t *next;

  while (f) {
    next = f->next;

    if (f->fd && f-> fd != stdout) 
      fclose(f->fd);
    else if (f->gzfd)
      gzclose(f->gzfd);
    else if (f->bz2fd)
      BZ2_bzclose(f->bz2fd);

    f = next;
  }
  return;
}
