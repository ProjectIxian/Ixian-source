#pragma once

#include <string.h>
#include <tomcrypt.h>

class IC_PRNG {
private:
	unsigned char *currentRandomState;
	unsigned int stateLen;
	symmetric_CTR *tomCipher;
	unsigned char* currentBuffer;
	unsigned int currentIndex;
	unsigned int currentKey;
public:
	IC_PRNG();
	~IC_PRNG();
public:
	/*
	* Returns:
	* 0		- OK
	* -1	- invalid length (must be %64)
	*/
	int setEntropy(unsigned char* entropy_data, unsigned int entropy_len);
	void dropEntropy();
	/*
	* Returns: 
	* 0		- OK
	* -1	- No Entropy State
	* -2	- Invalid TOMCRYPT hash
	* -3	- Invalid TOMCRYPT cipher
	* -4	- Problem initializing TOMCRYPT cipher
	*/
	void getBytes(unsigned char* out_buffer, unsigned int len);
private:
	/*
	* Returns:
	* 0		- OK
	* -3	- Invalid TOMCRYPT cipher
	* -4	- Problem initializing TOMCRYPT cipher
	*/
	int constructGenerator();
	void dropGenerator();
	void generateBlock();
};

extern "C" 
{
	void test_random(unsigned char* entropy, unsigned int entropy_len, int iteration, unsigned char* out_buffer, unsigned int out_len);
}