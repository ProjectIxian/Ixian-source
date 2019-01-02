/* LibTomCrypt, modular cryptographic library -- Tom St Denis
 *
 * LibTomCrypt is a library that provides various cryptographic
 * algorithms in a highly modular and flexible manner.
 *
 * The library is free for all purposes without any express
 * guarantee it works.
 */
#include "tomcrypt_private.h"

/**
  @file ctr_decrypt.c
  CTR implementation, decrypt data, Tom St Denis
*/

#ifdef LTC_CTR_MODE

/**
   CTR decrypt
   @param ct      Ciphertext
   @param pt      [out] Plaintext
   @param len     Length of ciphertext (octets)
   @param ctr     CTR state
   @return CRYPT_OK if successful
*/
int ctr_decrypt(const unsigned char *ct, unsigned char *pt, unsigned long len, symmetric_CTR *ctr)
{
   LTC_ARGCHK(pt != NULL);
   LTC_ARGCHK(ct != NULL);
   LTC_ARGCHK(ctr != NULL);

   return ctr_encrypt(ct, pt, len, ctr);
}

#endif


/* ref:         HEAD -> develop */
/* git commit:  01c455c3d5f781312de84594a11e102a20d5b959 */
/* commit time: 2018-12-17 15:44:02 +0100 */
