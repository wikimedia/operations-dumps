#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/types.h>
#include <regex.h>
#include "bzlib_private.h"
#include "bzlib.h"

/*---------------------------------------------------*/
/* Return  True iff data corruption is discovered.
   Returns False if there is no problem.
*/
Bool unRLE_obuf_to_output_FAST ( DState* s )
{
  UChar k1;

  if (s->blockRandomised) {

    while (True) {
      /* try to finish existing run */
      while (True) {
	if (s->strm->avail_out == 0) return False;
	if (s->state_out_len == 0) break;
	*( (UChar*)(s->strm->next_out) ) = s->state_out_ch;
	BZ_UPDATE_CRC ( s->calculatedBlockCRC, s->state_out_ch );
	s->state_out_len--;
	s->strm->next_out++;
	s->strm->avail_out--;
	s->strm->total_out_lo32++;
	if (s->strm->total_out_lo32 == 0) s->strm->total_out_hi32++;
      }

      /* can a new run be started? */
      if (s->nblock_used == s->save_nblock+1) return False;
               
      /* Only caused by corrupt data stream? */
      if (s->nblock_used > s->save_nblock+1)
	return True;
   
      s->state_out_len = 1;
      s->state_out_ch = s->k0;
      BZ_GET_FAST(k1); BZ_RAND_UPD_MASK; 
      k1 ^= BZ_RAND_MASK; s->nblock_used++;
      if (s->nblock_used == s->save_nblock+1) continue;
      if (k1 != s->k0) { s->k0 = k1; continue; };
   
      s->state_out_len = 2;
      BZ_GET_FAST(k1); BZ_RAND_UPD_MASK; 
      k1 ^= BZ_RAND_MASK; s->nblock_used++;
      if (s->nblock_used == s->save_nblock+1) continue;
      if (k1 != s->k0) { s->k0 = k1; continue; };
   
      s->state_out_len = 3;
      BZ_GET_FAST(k1); BZ_RAND_UPD_MASK; 
      k1 ^= BZ_RAND_MASK; s->nblock_used++;
      if (s->nblock_used == s->save_nblock+1) continue;
      if (k1 != s->k0) { s->k0 = k1; continue; };
   
      BZ_GET_FAST(k1); BZ_RAND_UPD_MASK; 
      k1 ^= BZ_RAND_MASK; s->nblock_used++;
      s->state_out_len = ((Int32)k1) + 4;
      BZ_GET_FAST(s->k0); BZ_RAND_UPD_MASK; 
      s->k0 ^= BZ_RAND_MASK; s->nblock_used++;
    }

  } else {

    /* restore */
    UInt32        c_calculatedBlockCRC = s->calculatedBlockCRC;
    UChar         c_state_out_ch       = s->state_out_ch;
    Int32         c_state_out_len      = s->state_out_len;
    Int32         c_nblock_used        = s->nblock_used;
    Int32         c_k0                 = s->k0;
    UInt32*       c_tt                 = s->tt;
    UInt32        c_tPos               = s->tPos;
    char*         cs_next_out          = s->strm->next_out;
    unsigned int  cs_avail_out         = s->strm->avail_out;
    Int32         ro_blockSize100k     = s->blockSize100k;
    /* end restore */

    UInt32       avail_out_INIT = cs_avail_out;
    Int32        s_save_nblockPP = s->save_nblock+1;
    unsigned int total_out_lo32_old;

    while (True) {

      /* try to finish existing run */
      if (c_state_out_len > 0) {
	while (True) {
	  if (cs_avail_out == 0) goto return_notr;
	  if (c_state_out_len == 1) break;
	  *( (UChar*)(cs_next_out) ) = c_state_out_ch;
	  BZ_UPDATE_CRC ( c_calculatedBlockCRC, c_state_out_ch );
	  c_state_out_len--;
	  cs_next_out++;
	  cs_avail_out--;
	}
      s_state_out_len_eq_one:
	{
	  if (cs_avail_out == 0) { 
	    c_state_out_len = 1; goto return_notr;
	  };
	  *( (UChar*)(cs_next_out) ) = c_state_out_ch;
	  BZ_UPDATE_CRC ( c_calculatedBlockCRC, c_state_out_ch );
	  cs_next_out++;
	  cs_avail_out--;
	}
      }   
      /* Only caused by corrupt data stream? */
      if (c_nblock_used > s_save_nblockPP)
	return True;

      /* can a new run be started? */
      if (c_nblock_used == s_save_nblockPP) {
	c_state_out_len = 0; goto return_notr;
      };   
      c_state_out_ch = c_k0;
      BZ_GET_FAST_C(k1); c_nblock_used++;
      if (k1 != c_k0) { 
	c_k0 = k1; goto s_state_out_len_eq_one; 
      };
      if (c_nblock_used == s_save_nblockPP) 
	goto s_state_out_len_eq_one;
   
      c_state_out_len = 2;
      BZ_GET_FAST_C(k1); c_nblock_used++;
      if (c_nblock_used == s_save_nblockPP) continue;
      if (k1 != c_k0) { c_k0 = k1; continue; };
   
      c_state_out_len = 3;
      BZ_GET_FAST_C(k1); c_nblock_used++;
      if (c_nblock_used == s_save_nblockPP) continue;
      if (k1 != c_k0) { c_k0 = k1; continue; };
   
      BZ_GET_FAST_C(k1); c_nblock_used++;
      c_state_out_len = ((Int32)k1) + 4;
      BZ_GET_FAST_C(c_k0); c_nblock_used++;
    }

  return_notr:
    total_out_lo32_old = s->strm->total_out_lo32;
    s->strm->total_out_lo32 += (avail_out_INIT - cs_avail_out);
    if (s->strm->total_out_lo32 < total_out_lo32_old)
      s->strm->total_out_hi32++;

    /* save */
    s->calculatedBlockCRC = c_calculatedBlockCRC;
    s->state_out_ch       = c_state_out_ch;
    s->state_out_len      = c_state_out_len;
    s->nblock_used        = c_nblock_used;
    s->k0                 = c_k0;
    s->tt                 = c_tt;
    s->tPos               = c_tPos;
    s->strm->next_out     = cs_next_out;
    s->strm->avail_out    = cs_avail_out;
    /* end save */
  }
  return False;
}

int BZ_API(BZ2_bzDecompress_mine) ( bz_stream *strm )
{
  Bool    corrupt;
  DState* s;
  if (strm == NULL) return BZ_PARAM_ERROR;
  s = strm->state;
  if (s == NULL) return BZ_PARAM_ERROR;
  if (s->strm != strm) return BZ_PARAM_ERROR;

  while (True) {
    if (s->state == BZ_X_IDLE) return BZ_SEQUENCE_ERROR;
    if (s->state == BZ_X_OUTPUT) {
      /*      if (s->smallDecompress)
	corrupt = unRLE_obuf_to_output_SMALL ( s ); else
	corrupt = unRLE_obuf_to_output_FAST  ( s ); */

      corrupt = unRLE_obuf_to_output_FAST  ( s ); 
      if (corrupt) return BZ_DATA_ERROR;
      if (s->nblock_used == s->save_nblock+1 && s->state_out_len == 0) {
	BZ_FINALISE_CRC ( s->calculatedBlockCRC );
	if (s->verbosity >= 3)
	  VPrintf2 ( " {0x%08x, 0x%08x}", s->storedBlockCRC,
		     s->calculatedBlockCRC );
	if (s->verbosity >= 2) VPrintf0 ( "]" );
	if (s->calculatedBlockCRC != s->storedBlockCRC)
	  return BZ_DATA_ERROR;
            s->calculatedCombinedCRC
	      = (s->calculatedCombinedCRC << 1) |
	      (s->calculatedCombinedCRC >> 31);
            s->calculatedCombinedCRC ^= s->calculatedBlockCRC;
            s->state = BZ_X_BLKHDR_1;
      } else {
	return BZ_OK;
      }
    }
    if (s->state >= BZ_X_MAGIC_1) {
      Int32 r = BZ2_decompress ( s );
      if (r == BZ_STREAM_END) {
	if (s->verbosity >= 3)
	  VPrintf2 ( "\n    combined CRCs: stored = 0x%08x, computed = 0x%08x",
		     s->storedCombinedCRC, s->calculatedCombinedCRC );
	/*	if (s->calculatedCombinedCRC != s->storedCombinedCRC)
		return BZ_DATA_ERROR; */
	return r;
      }
      if (s->state != BZ_X_OUTPUT) return r;
    }
  }

  AssertH ( 0, 6001 );

  return 0;  /*NOTREACHED*/
}
