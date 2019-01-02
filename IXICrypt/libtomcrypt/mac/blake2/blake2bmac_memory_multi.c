/* LibTomCrypt, modular cryptographic library -- Tom St Denis
 *
 * LibTomCrypt is a library that provides various cryptographic
 * algorithms in a highly modular and flexible manner.
 *
 * The library is free for all purposes without any express
 * guarantee it works.
 */

#include "tomcrypt_private.h"
#include <stdarg.h>

#ifdef LTC_BLAKE2BMAC

/**
   BLAKE2B MAC multiple blocks of memory to produce the authentication tag
   @param key       The secret key
   @param keylen    The length of the secret key (octets)
   @param mac       [out] Destination of the authentication tag
   @param maclen    [in/out] Max size and resulting size of authentication tag
   @param in        The data to BLAKE2B MAC
   @param inlen     The length of the data to BLAKE2B MAC (octets)
   @param ...       tuples of (data,len) pairs to BLAKE2B MAC, terminated with a (NULL,x) (x=don't care)
   @return CRYPT_OK if successful
*/
int blake2bmac_memory_multi(const unsigned char *key, unsigned long keylen, unsigned char *mac, unsigned long *maclen, const unsigned char *in,  unsigned long inlen, ...)
{
   blake2bmac_state st;
   int err;
   va_list args;
   const unsigned char *curptr;
   unsigned long curlen;

   LTC_ARGCHK(key    != NULL);
   LTC_ARGCHK(in     != NULL);
   LTC_ARGCHK(mac    != NULL);
   LTC_ARGCHK(maclen != NULL);

   va_start(args, inlen);
   curptr = in;
   curlen = inlen;
   if ((err = blake2bmac_init(&st, *maclen, key, keylen)) != CRYPT_OK)          { goto LBL_ERR; }
   for (;;) {
      if ((err = blake2bmac_process(&st, curptr, curlen)) != CRYPT_OK) { goto LBL_ERR; }
      curptr = va_arg(args, const unsigned char*);
      if (curptr == NULL) break;
      curlen = va_arg(args, unsigned long);
   }
   err = blake2bmac_done(&st, mac, maclen);
LBL_ERR:
#ifdef LTC_CLEAN_STACK
   zeromem(&st, sizeof(blake2bmac_state));
#endif
   va_end(args);
   return err;
}

#endif

/* ref:         HEAD -> develop */
/* git commit:  01c455c3d5f781312de84594a11e102a20d5b959 */
/* commit time: 2018-12-17 15:44:02 +0100 */
