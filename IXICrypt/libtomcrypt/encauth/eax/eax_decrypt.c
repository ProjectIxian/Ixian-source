/* LibTomCrypt, modular cryptographic library -- Tom St Denis
 *
 * LibTomCrypt is a library that provides various cryptographic
 * algorithms in a highly modular and flexible manner.
 *
 * The library is free for all purposes without any express
 * guarantee it works.
 */

/**
    @file eax_decrypt.c
    EAX implementation, decrypt block, by Tom St Denis
*/
#include "tomcrypt_private.h"

#ifdef LTC_EAX_MODE

/**
   Decrypt data with the EAX protocol
   @param eax     The EAX state
   @param ct      The ciphertext
   @param pt      [out] The plaintext
   @param length  The length (octets) of the ciphertext
   @return CRYPT_OK if successful
*/
int eax_decrypt(eax_state *eax, const unsigned char *ct, unsigned char *pt,
                unsigned long length)
{
   int err;

   LTC_ARGCHK(eax != NULL);
   LTC_ARGCHK(pt  != NULL);
   LTC_ARGCHK(ct  != NULL);

   /* omac ciphertext */
   if ((err = omac_process(&eax->ctomac, ct, length)) != CRYPT_OK) {
      return err;
   }

   /* decrypt  */
   return ctr_decrypt(ct, pt, length, &eax->ctr);
}

#endif

/* ref:         HEAD -> develop */
/* git commit:  01c455c3d5f781312de84594a11e102a20d5b959 */
/* commit time: 2018-12-17 15:44:02 +0100 */
