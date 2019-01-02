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
  Source donated by Elliptic Semiconductor Inc (www.ellipticsemi.com) to the LibTom Projects
*/

#ifdef LTC_XTS_MODE

/** Terminate XTS state
   @param xts    The state to terminate
*/
void xts_done(symmetric_xts *xts)
{
   LTC_ARGCHKVD(xts != NULL);
   cipher_descriptor[xts->cipher].done(&xts->key1);
   cipher_descriptor[xts->cipher].done(&xts->key2);
}

#endif

/* ref:         HEAD -> develop */
/* git commit:  01c455c3d5f781312de84594a11e102a20d5b959 */
/* commit time: 2018-12-17 15:44:02 +0100 */
