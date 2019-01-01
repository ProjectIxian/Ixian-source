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
  @file ecc_get_size.c
  ECC Crypto, Tom St Denis
*/

#ifdef LTC_MECC

/**
  Get the size of an ECC key
  @param key    The key to get the size of
  @return The size (octets) of the key or INT_MAX on error
*/
int ecc_get_size(const ecc_key *key)
{
   if (key == NULL) {
      return INT_MAX;
   }
   return key->dp.size;
}

#endif
/* ref:         HEAD -> develop */
/* git commit:  01c455c3d5f781312de84594a11e102a20d5b959 */
/* commit time: 2018-12-17 15:44:02 +0100 */

