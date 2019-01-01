#include "IC_PRNG.h"
#include "IXICrypt.h"
#include <tfm.h>

unsigned long ix_prng_read(unsigned char* out, unsigned long outlen, prng_state *prng);
const struct ltc_prng_descriptor ixiprng_desc = 
{
	"ixiprng", 0,
	0, // start
	0, // add_entropy
	0, // ready
	&ix_prng_read, //read
	0, // done
	0, // export
	0, // import
	0, // test
};

unsigned long ix_prng_read(unsigned char* out, unsigned long outlen, prng_state *prng)
{
	IC_PRNG *ic_prng = (IC_PRNG*)prng;
	ic_prng->getBytes(out, (unsigned int)outlen);
	return outlen;
}

IXI_RSA_KEY* ix_generate_rsa(unsigned char* entropy, unsigned int entropy_len, int key_size_bits, unsigned long pub_exponent)
{
	//printf("Generating RSA key: entropy (%d bytes), key bits: %d, exponent: %d.\n", entropy_len, key_size_bits, pub_exponent);
	//printf("Preparing PRNG...\n");
	// register required primitives
	if (find_prng("ixiprng") == -1)
	{
		register_prng(&ixiprng_desc);
		register_prng(&sprng_desc);
	}
	if (find_hash("sha512") == -1)
	{
		register_hash(&sha512_desc);
	}
	if (find_cipher("aes") == -1)
	{
		register_cipher(&aes_desc);
	}
	IC_PRNG prng;
	prng.setEntropy(entropy, entropy_len);
	Rsa_key tc_key;
	//printf("Calling libtomcrypt make key...\n");
	ltc_mp = tfm_desc;
	int result = rsa_make_key((prng_state*)&prng, find_prng("ixiprng"), key_size_bits / 8, (long)pub_exponent, &tc_key);
	//int result = rsa_make_key(NULL, find_prng("sprng"), key_size_bits / 8, (long)pub_exponent, &tc_key);
	//printf("Result: %d\n", result);
	if (result == CRYPT_OK)
	{
		IXI_RSA_KEY *export_ixikey = new IXI_RSA_KEY();
		unsigned char pkcs1_export[65536];
		unsigned long export_len = 65536;
		//printf("Exporting from libtomcrypt into PKCS#1...\n");
		rsa_export(pkcs1_export, &export_len, PK_PRIVATE, &tc_key);
		//printf("Export blobl len: %d.\n", export_len);
		export_ixikey->len = (unsigned int)export_len;
		export_ixikey->bytes = new unsigned char[export_ixikey->len];
		memcpy(export_ixikey->bytes, pkcs1_export, export_ixikey->len);
		rsa_free(&tc_key);
		//printf("Done, returning... exported address: %p\n", export_ixikey);
		return export_ixikey;
	}
	return 0;
}

void ix_free_key(IXI_RSA_KEY* key)
{
	//printf("Called Free on RSA exported structure: %p\n", key);
	if (key != 0) 
	{
		delete key;
	}
}