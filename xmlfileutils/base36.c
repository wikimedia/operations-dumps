#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/*
  these routines are used solely to convert a sha1 string
  to base336 for mediawiki revision table entries. grrrrr
  what a waste
*/

/* 
   args:
     in       array of ints, 3 bytes per int (leave the upper 4th byte
              free, it's needed for overflow for multibyte calculations)
     in_copy  pre-allocated array same size as in, which will be
              altered during the conversion and can be ignored afterwards
     temp     pre-allocated array same size as in, which will be used
              for temp results durin the conversion and can be ignored
              afterwards
     in_len   length of integer array
     out      pre-allocated array of integers into which the result
              will be placed, one base-36 digit per int.  no checks
              are made as to the length being sufficient, this is the
              caller's responsibility, strlen(in)*24/5 +1 (length needed
	      for base 32) should be enough

   returns:
      number of base 36 digits in the result

   this function converts an integer array to an array of base 36 digits.
   the input value is not altered
   the argument out will contain the result
*/
int tobase36(unsigned int *in, unsigned int *in_copy, unsigned int *temp, int in_len, unsigned int *out) {
  unsigned int digits;
  int overflow;

  int temp_ind = 0, in_ind = 0, out_ind = 0;
  int i;
  int done = 0;

  for (i=0; i<in_len; i++) in_copy[i] = in[i];

  while (1) {
    in_ind = temp_ind = overflow = 0;
    while (!in_copy[in_ind] && (in_ind < in_len)) in_ind++;
    
    while (in_ind < in_len) {
      if (in_copy[in_ind] < 36) {
	overflow = in_copy[in_ind++];
	if (in_len == 1) {
	  done++;
	  break;
	}
      }
      digits = overflow << 24 | in_copy[in_ind++];
      temp[temp_ind++] = digits / 36;
      overflow = digits % 36;
    }
    out[out_ind++] = overflow;
    if (done) {
      /* reverse the digits now */
      for (i = 0; i< out_ind; i++) temp[i] = out[i];
      for (i = 0; i< out_ind; i++) out[out_ind - i -1] = temp[i];
      return(out_ind);
    }
    for (i=0; i<temp_ind; i++) in_copy[i] = temp[i];
    in_len = temp_ind;
  }
}

/* 
   args:
     c       character representing a hex digit, lower case

   returns   corresponding integer value

   this function converts a single char (interpreted as
   a hex digit) to int
*/
int char2int(char c) {
  char *map="0123456789abcdef";

  return(strchr(map, c) - map);
}

/*
  args:
     s           character string representing hex digits
     len         length of s (it does not need to be null-terminated) 
     intbuf      pre-allocated array of integers into which the result
                 will be placed, 3 bytes per int.  no checks
                 are made as to the length being sufficient, this is the
                 caller's responsibility, strlen(s)/6 + 1 is enough

  returns:
    length of int buf used

  this function packs an array of characters representing hex digits
  into an array of ints, 3 bytes per int
*/
int hexstring2int(char *s, int len, unsigned int *intbuf) {
  int s_ind = 0, int_ind = 0;
  int remainder;
  int i;

  remainder = len%6;
  int_ind = 0;
  intbuf[int_ind] = 0;
  while (remainder && s_ind < len) {
    intbuf[int_ind] = char2int(s[s_ind++]) | (intbuf[int_ind] << 4);
    remainder -=1;
    len-=1;
  }
  if (intbuf[int_ind]) int_ind++;
  
  while (len>0) {
    intbuf[int_ind] = 0;
    for (i=0; i<6; i++) {
      intbuf[int_ind] = char2int(s[s_ind++]) | (intbuf[int_ind] << 4);
    }    
    len -=6;
    int_ind++;
  }
  return(int_ind);
}

/*
  args:
     i      integer to covert

  returns:
     character corresponding to the base-36 value of the int

  this function converts a single integer (of value less than 36) to
  its character representation
*/
char int2char(int i) {
  char *map="0123456789abcdefghijklmnopqrstuvwxyz";

  return(map[i]);
}

/* 
   args:
      int_buf        array of ints, one base-36 digit per int
      int_buf_len    length of array of ints
      s              pre-allocated buffer into which the representation
                     of the int array will be placed.  the string is
		     the null-terminated. no check is made to determine
		     that the buffer is large enough, that is the caller's
		     responsibility, int_buf_len + 1 is enough.

   this function converts an array of integers, each element representing
   a base-36 value, into a character string representing the array
   leading 0's in the integer value are omitted
*/
void int2string(unsigned int *int_buf, int int_buf_len, char *s) {
  int int_buf_ind = 0, s_ind = 0;

  /* skip leading 0's */
  while (!int_buf[int_buf_ind] && int_buf_ind<int_buf_len) int_buf_ind++;

  while (int_buf_ind < int_buf_len) {
    s[s_ind] = int2char(int_buf[int_buf_ind]);
    s_ind++;
    int_buf_ind++;
  }
  s[s_ind] = '\0';
  return;
}

/*
  typical usage: 

int main() {
  char s_in[41];
  int s_in_len;
  unsigned int copy[41];
  unsigned int temp[41];

  unsigned int num_buf[7];
  int num_buf_len;

  unsigned int output[34];
  char s_out[35];

  int out_len;

  strcpy(s_in, "560913458ecab77ad7989fa33fa4e5ddce2b367e");
  s_in_len = strlen(s_in);
  num_buf_len = hexstring2int(s_in, s_in_len, num_buf);
  out_len = tobase36(num_buf, copy, temp, num_buf_len, output);
  int2string(output, out_len, s_out);
  fprintf(stderr,"result is %s\n", s_out);
  exit(0);
}

*/
