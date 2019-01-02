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
  @file ecc_ansi_x963_export.c
  ECC Crypto, Tom St Denis
*/

#ifdef LTC_MECC

/** ECC X9.63 (Sec. 4.3.6) uncompressed export
  @param key     Key to export
  @param out     [out] destination of export
  @param outlen  [in/out]  Length of destination and final output size
  Return CRYPT_OK on success
*/
int ecc_ansi_x963_export(const ecc_key *key, unsigned char *out, unsigned long *outlen)
{
   return ecc_get_key(out, outlen, PK_PUBLIC, key);
}

#endif

/* ref:         HEAD -> develop */
/* git commit:  01c455c3d5f781312de84594a11e102a20d5b959 */
/* commit time: 2018-12-17 15:44:02 +0100 */
