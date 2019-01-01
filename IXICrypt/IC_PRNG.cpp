#include "IC_PRNG.h"

IC_PRNG::IC_PRNG()
{
	stateLen = 0;
	currentRandomState = 0;
	tomCipher = 0;
	currentBuffer = 0;
	currentIndex = 0;
	currentKey = 0;
}

IC_PRNG::~IC_PRNG()
{
	dropGenerator();
	dropEntropy();
}

int IC_PRNG::setEntropy(unsigned char* entropy_data, unsigned int entropy_len)
{
	dropGenerator();
	dropEntropy();
	if (entropy_len % 64 != 0) return -1;
	if (entropy_len == 0) return -1;
	stateLen = entropy_len;
	currentRandomState = new unsigned char[stateLen];
	memcpy(currentRandomState, entropy_data, stateLen);
	constructGenerator();
	return 0;
}

void IC_PRNG::dropEntropy()
{
	if (currentRandomState != 0)
	{
		delete[] currentRandomState;
		currentRandomState = 0;
	}
}

void IC_PRNG::getBytes(unsigned char* out_buffer, unsigned int len) 
{
	//printf("Requested %d random bytes.\n", len);
	unsigned int cpos = 0;
	while (cpos < len)
	{
		unsigned int avail = 16 - currentIndex;
		unsigned int need = len - cpos;
		//printf("-> Available in buffer: %d. Still needed: %d\n", avail, need);
		if (need <= avail) 
		{
			//printf("-> There is enough to satisfy the request.\n");
			memcpy(out_buffer + cpos, currentBuffer + currentIndex, need);
			cpos += need;
			currentIndex += need;
		}
		else 
		{
			//printf("There isn't enough to satisfy the request. Putting %d bytes, starting at %d, to buffer at destination %d\n", avail, currentIndex, cpos);
			memcpy(out_buffer + cpos, currentBuffer + currentIndex, avail);
			cpos += avail;
			generateBlock();
		}
	}
	//printf("Bytes generated. Press ENTER\n");
	//getchar();
}

int IC_PRNG::constructGenerator() 
{
	dropGenerator();
	//printf("Creating AES-CTR random generator.\n");
	int cipher = find_cipher("aes");
	//printf("Selected libtomcrypt cipher: %d\n", cipher);
	if (cipher < 0) return -3;
	tomCipher = new symmetric_CTR;
	unsigned char IV[16];
	memset(IV, 0, 16);
	//printf("Calling libtomcrypt ctr cipher...\n");
	int r = ctr_start(cipher, IV, currentRandomState, 16, 0, LTC_CTR_RFC3686, tomCipher);
	//printf("Call returns: %d.\n", r);
	if (r != 0)
	{
		delete tomCipher;
		tomCipher = 0;
		return -4;
	}
	generateBlock();
	return 0;
}

void IC_PRNG::dropGenerator() 
{
	//printf("Dropping AES-CTR generator.\n");
	if (tomCipher != 0) 
	{
		delete tomCipher;
		tomCipher = 0;
	}
	if (currentBuffer != 0) 
	{
		delete[] currentBuffer;
		currentBuffer = 0;
	}
	currentIndex = 0;
	currentKey = 0;
}

void IC_PRNG::generateBlock()
{
	//printf("Generating AES-CTR block... Current key offset: %d / %d\n", currentKey, stateLen);
	if (currentBuffer == 0) {
		currentBuffer = new unsigned char[16];
	}
	currentKey += 16;
	if (currentKey >= stateLen)
	{
		currentKey = 0;
	}
	ctr_encrypt(currentRandomState + currentKey, currentBuffer, 16, tomCipher);
	currentIndex = 0;
}