#pragma once

#ifdef _MSC_VER
#define IXI_EXPORT __declspec(dllexport)
#endif

struct IXI_RSA_KEY
{
	unsigned int len;
	unsigned char* bytes;
};

extern "C"
{
	IXI_EXPORT IXI_RSA_KEY* ix_generate_rsa(unsigned char* entropy, unsigned int entropy_len, int key_size_bits, unsigned long pub_exponent);
	IXI_EXPORT void ix_free_key(IXI_RSA_KEY* key);
}
