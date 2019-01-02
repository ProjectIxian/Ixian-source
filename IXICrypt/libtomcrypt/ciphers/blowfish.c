/* LibTomCrypt, modular cryptographic library -- Tom St Denis
 *
 * LibTomCrypt is a library that provides various cryptographic
 * algorithms in a highly modular and flexible manner.
 *
 * The library is free for all purposes without any express
 * guarantee it works.
 */
/**
  @file blowfish.c
  Implementation of the Blowfish block cipher, Tom St Denis
*/
#include "tomcrypt_private.h"

#ifdef LTC_BLOWFISH

const struct ltc_cipher_descriptor blowfish_desc =
{
    "blowfish",
    0,
    8, 56, 8, 16,
    &blowfish_setup,
    &blowfish_ecb_encrypt,
    &blowfish_ecb_decrypt,
    &blowfish_test,
    &blowfish_done,
    &blowfish_keysize,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
};

static const ulong32 ORIG_P[16 + 2] = {
        0x243F6A88UL, 0x85A308D3UL, 0x13198A2EUL, 0x03707344UL,
        0xA4093822UL, 0x299F31D0UL, 0x082EFA98UL, 0xEC4E6C89UL,
        0x452821E6UL, 0x38D01377UL, 0xBE5466CFUL, 0x34E90C6CUL,
        0xC0AC29B7UL, 0xC97C50DDUL, 0x3F84D5B5UL, 0xB5470917UL,
        0x9216D5D9UL, 0x8979FB1BUL
};

static const ulong32 ORIG_S[4][256] = {
    {   0xD1310BA6UL, 0x98DFB5ACUL, 0x2FFD72DBUL, 0xD01ADFB7UL,
        0xB8E1AFEDUL, 0x6A267E96UL, 0xBA7C9045UL, 0xF12C7F99UL,
        0x24A19947UL, 0xB3916CF7UL, 0x0801F2E2UL, 0x858EFC16UL,
        0x636920D8UL, 0x71574E69UL, 0xA458FEA3UL, 0xF4933D7EUL,
        0x0D95748FUL, 0x728EB658UL, 0x718BCD58UL, 0x82154AEEUL,
        0x7B54A41DUL, 0xC25A59B5UL, 0x9C30D539UL, 0x2AF26013UL,
        0xC5D1B023UL, 0x286085F0UL, 0xCA417918UL, 0xB8DB38EFUL,
        0x8E79DCB0UL, 0x603A180EUL, 0x6C9E0E8BUL, 0xB01E8A3EUL,
        0xD71577C1UL, 0xBD314B27UL, 0x78AF2FDAUL, 0x55605C60UL,
        0xE65525F3UL, 0xAA55AB94UL, 0x57489862UL, 0x63E81440UL,
        0x55CA396AUL, 0x2AAB10B6UL, 0xB4CC5C34UL, 0x1141E8CEUL,
        0xA15486AFUL, 0x7C72E993UL, 0xB3EE1411UL, 0x636FBC2AUL,
        0x2BA9C55DUL, 0x741831F6UL, 0xCE5C3E16UL, 0x9B87931EUL,
        0xAFD6BA33UL, 0x6C24CF5CUL, 0x7A325381UL, 0x28958677UL,
        0x3B8F4898UL, 0x6B4BB9AFUL, 0xC4BFE81BUL, 0x66282193UL,
        0x61D809CCUL, 0xFB21A991UL, 0x487CAC60UL, 0x5DEC8032UL,
        0xEF845D5DUL, 0xE98575B1UL, 0xDC262302UL, 0xEB651B88UL,
        0x23893E81UL, 0xD396ACC5UL, 0x0F6D6FF3UL, 0x83F44239UL,
        0x2E0B4482UL, 0xA4842004UL, 0x69C8F04AUL, 0x9E1F9B5EUL,
        0x21C66842UL, 0xF6E96C9AUL, 0x670C9C61UL, 0xABD388F0UL,
        0x6A51A0D2UL, 0xD8542F68UL, 0x960FA728UL, 0xAB5133A3UL,
        0x6EEF0B6CUL, 0x137A3BE4UL, 0xBA3BF050UL, 0x7EFB2A98UL,
        0xA1F1651DUL, 0x39AF0176UL, 0x66CA593EUL, 0x82430E88UL,
        0x8CEE8619UL, 0x456F9FB4UL, 0x7D84A5C3UL, 0x3B8B5EBEUL,
        0xE06F75D8UL, 0x85C12073UL, 0x401A449FUL, 0x56C16AA6UL,
        0x4ED3AA62UL, 0x363F7706UL, 0x1BFEDF72UL, 0x429B023DUL,
        0x37D0D724UL, 0xD00A1248UL, 0xDB0FEAD3UL, 0x49F1C09BUL,
        0x075372C9UL, 0x80991B7BUL, 0x25D479D8UL, 0xF6E8DEF7UL,
        0xE3FE501AUL, 0xB6794C3BUL, 0x976CE0BDUL, 0x04C006BAUL,
        0xC1A94FB6UL, 0x409F60C4UL, 0x5E5C9EC2UL, 0x196A2463UL,
        0x68FB6FAFUL, 0x3E6C53B5UL, 0x1339B2EBUL, 0x3B52EC6FUL,
        0x6DFC511FUL, 0x9B30952CUL, 0xCC814544UL, 0xAF5EBD09UL,
        0xBEE3D004UL, 0xDE334AFDUL, 0x660F2807UL, 0x192E4BB3UL,
        0xC0CBA857UL, 0x45C8740FUL, 0xD20B5F39UL, 0xB9D3FBDBUL,
        0x5579C0BDUL, 0x1A60320AUL, 0xD6A100C6UL, 0x402C7279UL,
        0x679F25FEUL, 0xFB1FA3CCUL, 0x8EA5E9F8UL, 0xDB3222F8UL,
        0x3C7516DFUL, 0xFD616B15UL, 0x2F501EC8UL, 0xAD0552ABUL,
        0x323DB5FAUL, 0xFD238760UL, 0x53317B48UL, 0x3E00DF82UL,
        0x9E5C57BBUL, 0xCA6F8CA0UL, 0x1A87562EUL, 0xDF1769DBUL,
        0xD542A8F6UL, 0x287EFFC3UL, 0xAC6732C6UL, 0x8C4F5573UL,
        0x695B27B0UL, 0xBBCA58C8UL, 0xE1FFA35DUL, 0xB8F011A0UL,
        0x10FA3D98UL, 0xFD2183B8UL, 0x4AFCB56CUL, 0x2DD1D35BUL,
        0x9A53E479UL, 0xB6F84565UL, 0xD28E49BCUL, 0x4BFB9790UL,
        0xE1DDF2DAUL, 0xA4CB7E33UL, 0x62FB1341UL, 0xCEE4C6E8UL,
        0xEF20CADAUL, 0x36774C01UL, 0xD07E9EFEUL, 0x2BF11FB4UL,
        0x95DBDA4DUL, 0xAE909198UL, 0xEAAD8E71UL, 0x6B93D5A0UL,
        0xD08ED1D0UL, 0xAFC725E0UL, 0x8E3C5B2FUL, 0x8E7594B7UL,
        0x8FF6E2FBUL, 0xF2122B64UL, 0x8888B812UL, 0x900DF01CUL,
        0x4FAD5EA0UL, 0x688FC31CUL, 0xD1CFF191UL, 0xB3A8C1ADUL,
        0x2F2F2218UL, 0xBE0E1777UL, 0xEA752DFEUL, 0x8B021FA1UL,
        0xE5A0CC0FUL, 0xB56F74E8UL, 0x18ACF3D6UL, 0xCE89E299UL,
        0xB4A84FE0UL, 0xFD13E0B7UL, 0x7CC43B81UL, 0xD2ADA8D9UL,
        0x165FA266UL, 0x80957705UL, 0x93CC7314UL, 0x211A1477UL,
        0xE6AD2065UL, 0x77B5FA86UL, 0xC75442F5UL, 0xFB9D35CFUL,
        0xEBCDAF0CUL, 0x7B3E89A0UL, 0xD6411BD3UL, 0xAE1E7E49UL,
        0x00250E2DUL, 0x2071B35EUL, 0x226800BBUL, 0x57B8E0AFUL,
        0x2464369BUL, 0xF009B91EUL, 0x5563911DUL, 0x59DFA6AAUL,
        0x78C14389UL, 0xD95A537FUL, 0x207D5BA2UL, 0x02E5B9C5UL,
        0x83260376UL, 0x6295CFA9UL, 0x11C81968UL, 0x4E734A41UL,
        0xB3472DCAUL, 0x7B14A94AUL, 0x1B510052UL, 0x9A532915UL,
        0xD60F573FUL, 0xBC9BC6E4UL, 0x2B60A476UL, 0x81E67400UL,
        0x08BA6FB5UL, 0x571BE91FUL, 0xF296EC6BUL, 0x2A0DD915UL,
        0xB6636521UL, 0xE7B9F9B6UL, 0xFF34052EUL, 0xC5855664UL,
        0x53B02D5DUL, 0xA99F8FA1UL, 0x08BA4799UL, 0x6E85076AUL   },
    {   0x4B7A70E9UL, 0xB5B32944UL, 0xDB75092EUL, 0xC4192623UL,
        0xAD6EA6B0UL, 0x49A7DF7DUL, 0x9CEE60B8UL, 0x8FEDB266UL,
        0xECAA8C71UL, 0x699A17FFUL, 0x5664526CUL, 0xC2B19EE1UL,
        0x193602A5UL, 0x75094C29UL, 0xA0591340UL, 0xE4183A3EUL,
        0x3F54989AUL, 0x5B429D65UL, 0x6B8FE4D6UL, 0x99F73FD6UL,
        0xA1D29C07UL, 0xEFE830F5UL, 0x4D2D38E6UL, 0xF0255DC1UL,
        0x4CDD2086UL, 0x8470EB26UL, 0x6382E9C6UL, 0x021ECC5EUL,
        0x09686B3FUL, 0x3EBAEFC9UL, 0x3C971814UL, 0x6B6A70A1UL,
        0x687F3584UL, 0x52A0E286UL, 0xB79C5305UL, 0xAA500737UL,
        0x3E07841CUL, 0x7FDEAE5CUL, 0x8E7D44ECUL, 0x5716F2B8UL,
        0xB03ADA37UL, 0xF0500C0DUL, 0xF01C1F04UL, 0x0200B3FFUL,
        0xAE0CF51AUL, 0x3CB574B2UL, 0x25837A58UL, 0xDC0921BDUL,
        0xD19113F9UL, 0x7CA92FF6UL, 0x94324773UL, 0x22F54701UL,
        0x3AE5E581UL, 0x37C2DADCUL, 0xC8B57634UL, 0x9AF3DDA7UL,
        0xA9446146UL, 0x0FD0030EUL, 0xECC8C73EUL, 0xA4751E41UL,
        0xE238CD99UL, 0x3BEA0E2FUL, 0x3280BBA1UL, 0x183EB331UL,
        0x4E548B38UL, 0x4F6DB908UL, 0x6F420D03UL, 0xF60A04BFUL,
        0x2CB81290UL, 0x24977C79UL, 0x5679B072UL, 0xBCAF89AFUL,
        0xDE9A771FUL, 0xD9930810UL, 0xB38BAE12UL, 0xDCCF3F2EUL,
        0x5512721FUL, 0x2E6B7124UL, 0x501ADDE6UL, 0x9F84CD87UL,
        0x7A584718UL, 0x7408DA17UL, 0xBC9F9ABCUL, 0xE94B7D8CUL,
        0xEC7AEC3AUL, 0xDB851DFAUL, 0x63094366UL, 0xC464C3D2UL,
        0xEF1C1847UL, 0x3215D908UL, 0xDD433B37UL, 0x24C2BA16UL,
        0x12A14D43UL, 0x2A65C451UL, 0x50940002UL, 0x133AE4DDUL,
        0x71DFF89EUL, 0x10314E55UL, 0x81AC77D6UL, 0x5F11199BUL,
        0x043556F1UL, 0xD7A3C76BUL, 0x3C11183BUL, 0x5924A509UL,
        0xF28FE6EDUL, 0x97F1FBFAUL, 0x9EBABF2CUL, 0x1E153C6EUL,
        0x86E34570UL, 0xEAE96FB1UL, 0x860E5E0AUL, 0x5A3E2AB3UL,
        0x771FE71CUL, 0x4E3D06FAUL, 0x2965DCB9UL, 0x99E71D0FUL,
        0x803E89D6UL, 0x5266C825UL, 0x2E4CC978UL, 0x9C10B36AUL,
        0xC6150EBAUL, 0x94E2EA78UL, 0xA5FC3C53UL, 0x1E0A2DF4UL,
        0xF2F74EA7UL, 0x361D2B3DUL, 0x1939260FUL, 0x19C27960UL,
        0x5223A708UL, 0xF71312B6UL, 0xEBADFE6EUL, 0xEAC31F66UL,
        0xE3BC4595UL, 0xA67BC883UL, 0xB17F37D1UL, 0x018CFF28UL,
        0xC332DDEFUL, 0xBE6C5AA5UL, 0x65582185UL, 0x68AB9802UL,
        0xEECEA50FUL, 0xDB2F953BUL, 0x2AEF7DADUL, 0x5B6E2F84UL,
        0x1521B628UL, 0x29076170UL, 0xECDD4775UL, 0x619F1510UL,
        0x13CCA830UL, 0xEB61BD96UL, 0x0334FE1EUL, 0xAA0363CFUL,
        0xB5735C90UL, 0x4C70A239UL, 0xD59E9E0BUL, 0xCBAADE14UL,
        0xEECC86BCUL, 0x60622CA7UL, 0x9CAB5CABUL, 0xB2F3846EUL,
        0x648B1EAFUL, 0x19BDF0CAUL, 0xA02369B9UL, 0x655ABB50UL,
        0x40685A32UL, 0x3C2AB4B3UL, 0x319EE9D5UL, 0xC021B8F7UL,
        0x9B540B19UL, 0x875FA099UL, 0x95F7997EUL, 0x623D7DA8UL,
        0xF837889AUL, 0x97E32D77UL, 0x11ED935FUL, 0x16681281UL,
        0x0E358829UL, 0xC7E61FD6UL, 0x96DEDFA1UL, 0x7858BA99UL,
        0x57F584A5UL, 0x1B227263UL, 0x9B83C3FFUL, 0x1AC24696UL,
        0xCDB30AEBUL, 0x532E3054UL, 0x8FD948E4UL, 0x6DBC3128UL,
        0x58EBF2EFUL, 0x34C6FFEAUL, 0xFE28ED61UL, 0xEE7C3C73UL,
        0x5D4A14D9UL, 0xE864B7E3UL, 0x42105D14UL, 0x203E13E0UL,
        0x45EEE2B6UL, 0xA3AAABEAUL, 0xDB6C4F15UL, 0xFACB4FD0UL,
        0xC742F442UL, 0xEF6ABBB5UL, 0x654F3B1DUL, 0x41CD2105UL,
        0xD81E799EUL, 0x86854DC7UL, 0xE44B476AUL, 0x3D816250UL,
        0xCF62A1F2UL, 0x5B8D2646UL, 0xFC8883A0UL, 0xC1C7B6A3UL,
        0x7F1524C3UL, 0x69CB7492UL, 0x47848A0BUL, 0x5692B285UL,
        0x095BBF00UL, 0xAD19489DUL, 0x1462B174UL, 0x23820E00UL,
        0x58428D2AUL, 0x0C55F5EAUL, 0x1DADF43EUL, 0x233F7061UL,
        0x3372F092UL, 0x8D937E41UL, 0xD65FECF1UL, 0x6C223BDBUL,
        0x7CDE3759UL, 0xCBEE7460UL, 0x4085F2A7UL, 0xCE77326EUL,
        0xA6078084UL, 0x19F8509EUL, 0xE8EFD855UL, 0x61D99735UL,
        0xA969A7AAUL, 0xC50C06C2UL, 0x5A04ABFCUL, 0x800BCADCUL,
        0x9E447A2EUL, 0xC3453484UL, 0xFDD56705UL, 0x0E1E9EC9UL,
        0xDB73DBD3UL, 0x105588CDUL, 0x675FDA79UL, 0xE3674340UL,
        0xC5C43465UL, 0x713E38D8UL, 0x3D28F89EUL, 0xF16DFF20UL,
        0x153E21E7UL, 0x8FB03D4AUL, 0xE6E39F2BUL, 0xDB83ADF7UL   },
    {   0xE93D5A68UL, 0x948140F7UL, 0xF64C261CUL, 0x94692934UL,
        0x411520F7UL, 0x7602D4F7UL, 0xBCF46B2EUL, 0xD4A20068UL,
        0xD4082471UL, 0x3320F46AUL, 0x43B7D4B7UL, 0x500061AFUL,
        0x1E39F62EUL, 0x97244546UL, 0x14214F74UL, 0xBF8B8840UL,
        0x4D95FC1DUL, 0x96B591AFUL, 0x70F4DDD3UL, 0x66A02F45UL,
        0xBFBC09ECUL, 0x03BD9785UL, 0x7FAC6DD0UL, 0x31CB8504UL,
        0x96EB27B3UL, 0x55FD3941UL, 0xDA2547E6UL, 0xABCA0A9AUL,
        0x28507825UL, 0x530429F4UL, 0x0A2C86DAUL, 0xE9B66DFBUL,
        0x68DC1462UL, 0xD7486900UL, 0x680EC0A4UL, 0x27A18DEEUL,
        0x4F3FFEA2UL, 0xE887AD8CUL, 0xB58CE006UL, 0x7AF4D6B6UL,
        0xAACE1E7CUL, 0xD3375FECUL, 0xCE78A399UL, 0x406B2A42UL,
        0x20FE9E35UL, 0xD9F385B9UL, 0xEE39D7ABUL, 0x3B124E8BUL,
        0x1DC9FAF7UL, 0x4B6D1856UL, 0x26A36631UL, 0xEAE397B2UL,
        0x3A6EFA74UL, 0xDD5B4332UL, 0x6841E7F7UL, 0xCA7820FBUL,
        0xFB0AF54EUL, 0xD8FEB397UL, 0x454056ACUL, 0xBA489527UL,
        0x55533A3AUL, 0x20838D87UL, 0xFE6BA9B7UL, 0xD096954BUL,
        0x55A867BCUL, 0xA1159A58UL, 0xCCA92963UL, 0x99E1DB33UL,
        0xA62A4A56UL, 0x3F3125F9UL, 0x5EF47E1CUL, 0x9029317CUL,
        0xFDF8E802UL, 0x04272F70UL, 0x80BB155CUL, 0x05282CE3UL,
        0x95C11548UL, 0xE4C66D22UL, 0x48C1133FUL, 0xC70F86DCUL,
        0x07F9C9EEUL, 0x41041F0FUL, 0x404779A4UL, 0x5D886E17UL,
        0x325F51EBUL, 0xD59BC0D1UL, 0xF2BCC18FUL, 0x41113564UL,
        0x257B7834UL, 0x602A9C60UL, 0xDFF8E8A3UL, 0x1F636C1BUL,
        0x0E12B4C2UL, 0x02E1329EUL, 0xAF664FD1UL, 0xCAD18115UL,
        0x6B2395E0UL, 0x333E92E1UL, 0x3B240B62UL, 0xEEBEB922UL,
        0x85B2A20EUL, 0xE6BA0D99UL, 0xDE720C8CUL, 0x2DA2F728UL,
        0xD0127845UL, 0x95B794FDUL, 0x647D0862UL, 0xE7CCF5F0UL,
        0x5449A36FUL, 0x877D48FAUL, 0xC39DFD27UL, 0xF33E8D1EUL,
        0x0A476341UL, 0x992EFF74UL, 0x3A6F6EABUL, 0xF4F8FD37UL,
        0xA812DC60UL, 0xA1EBDDF8UL, 0x991BE14CUL, 0xDB6E6B0DUL,
        0xC67B5510UL, 0x6D672C37UL, 0x2765D43BUL, 0xDCD0E804UL,
        0xF1290DC7UL, 0xCC00FFA3UL, 0xB5390F92UL, 0x690FED0BUL,
        0x667B9FFBUL, 0xCEDB7D9CUL, 0xA091CF0BUL, 0xD9155EA3UL,
        0xBB132F88UL, 0x515BAD24UL, 0x7B9479BFUL, 0x763BD6EBUL,
        0x37392EB3UL, 0xCC115979UL, 0x8026E297UL, 0xF42E312DUL,
        0x6842ADA7UL, 0xC66A2B3BUL, 0x12754CCCUL, 0x782EF11CUL,
        0x6A124237UL, 0xB79251E7UL, 0x06A1BBE6UL, 0x4BFB6350UL,
        0x1A6B1018UL, 0x11CAEDFAUL, 0x3D25BDD8UL, 0xE2E1C3C9UL,
        0x44421659UL, 0x0A121386UL, 0xD90CEC6EUL, 0xD5ABEA2AUL,
        0x64AF674EUL, 0xDA86A85FUL, 0xBEBFE988UL, 0x64E4C3FEUL,
        0x9DBC8057UL, 0xF0F7C086UL, 0x60787BF8UL, 0x6003604DUL,
        0xD1FD8346UL, 0xF6381FB0UL, 0x7745AE04UL, 0xD736FCCCUL,
        0x83426B33UL, 0xF01EAB71UL, 0xB0804187UL, 0x3C005E5FUL,
        0x77A057BEUL, 0xBDE8AE24UL, 0x55464299UL, 0xBF582E61UL,
        0x4E58F48FUL, 0xF2DDFDA2UL, 0xF474EF38UL, 0x8789BDC2UL,
        0x5366F9C3UL, 0xC8B38E74UL, 0xB475F255UL, 0x46FCD9B9UL,
        0x7AEB2661UL, 0x8B1DDF84UL, 0x846A0E79UL, 0x915F95E2UL,
        0x466E598EUL, 0x20B45770UL, 0x8CD55591UL, 0xC902DE4CUL,
        0xB90BACE1UL, 0xBB8205D0UL, 0x11A86248UL, 0x7574A99EUL,
        0xB77F19B6UL, 0xE0A9DC09UL, 0x662D09A1UL, 0xC4324633UL,
        0xE85A1F02UL, 0x09F0BE8CUL, 0x4A99A025UL, 0x1D6EFE10UL,
        0x1AB93D1DUL, 0x0BA5A4DFUL, 0xA186F20FUL, 0x2868F169UL,
        0xDCB7DA83UL, 0x573906FEUL, 0xA1E2CE9BUL, 0x4FCD7F52UL,
        0x50115E01UL, 0xA70683FAUL, 0xA002B5C4UL, 0x0DE6D027UL,
        0x9AF88C27UL, 0x773F8641UL, 0xC3604C06UL, 0x61A806B5UL,
        0xF0177A28UL, 0xC0F586E0UL, 0x006058AAUL, 0x30DC7D62UL,
        0x11E69ED7UL, 0x2338EA63UL, 0x53C2DD94UL, 0xC2C21634UL,
        0xBBCBEE56UL, 0x90BCB6DEUL, 0xEBFC7DA1UL, 0xCE591D76UL,
        0x6F05E409UL, 0x4B7C0188UL, 0x39720A3DUL, 0x7C927C24UL,
        0x86E3725FUL, 0x724D9DB9UL, 0x1AC15BB4UL, 0xD39EB8FCUL,
        0xED545578UL, 0x08FCA5B5UL, 0xD83D7CD3UL, 0x4DAD0FC4UL,
        0x1E50EF5EUL, 0xB161E6F8UL, 0xA28514D9UL, 0x6C51133CUL,
        0x6FD5C7E7UL, 0x56E14EC4UL, 0x362ABFCEUL, 0xDDC6C837UL,
        0xD79A3234UL, 0x92638212UL, 0x670EFA8EUL, 0x406000E0UL  },
    {   0x3A39CE37UL, 0xD3FAF5CFUL, 0xABC27737UL, 0x5AC52D1BUL,
        0x5CB0679EUL, 0x4FA33742UL, 0xD3822740UL, 0x99BC9BBEUL,
        0xD5118E9DUL, 0xBF0F7315UL, 0xD62D1C7EUL, 0xC700C47BUL,
        0xB78C1B6BUL, 0x21A19045UL, 0xB26EB1BEUL, 0x6A366EB4UL,
        0x5748AB2FUL, 0xBC946E79UL, 0xC6A376D2UL, 0x6549C2C8UL,
        0x530FF8EEUL, 0x468DDE7DUL, 0xD5730A1DUL, 0x4CD04DC6UL,
        0x2939BBDBUL, 0xA9BA4650UL, 0xAC9526E8UL, 0xBE5EE304UL,
        0xA1FAD5F0UL, 0x6A2D519AUL, 0x63EF8CE2UL, 0x9A86EE22UL,
        0xC089C2B8UL, 0x43242EF6UL, 0xA51E03AAUL, 0x9CF2D0A4UL,
        0x83C061BAUL, 0x9BE96A4DUL, 0x8FE51550UL, 0xBA645BD6UL,
        0x2826A2F9UL, 0xA73A3AE1UL, 0x4BA99586UL, 0xEF5562E9UL,
        0xC72FEFD3UL, 0xF752F7DAUL, 0x3F046F69UL, 0x77FA0A59UL,
        0x80E4A915UL, 0x87B08601UL, 0x9B09E6ADUL, 0x3B3EE593UL,
        0xE990FD5AUL, 0x9E34D797UL, 0x2CF0B7D9UL, 0x022B8B51UL,
        0x96D5AC3AUL, 0x017DA67DUL, 0xD1CF3ED6UL, 0x7C7D2D28UL,
        0x1F9F25CFUL, 0xADF2B89BUL, 0x5AD6B472UL, 0x5A88F54CUL,
        0xE029AC71UL, 0xE019A5E6UL, 0x47B0ACFDUL, 0xED93FA9BUL,
        0xE8D3C48DUL, 0x283B57CCUL, 0xF8D56629UL, 0x79132E28UL,
        0x785F0191UL, 0xED756055UL, 0xF7960E44UL, 0xE3D35E8CUL,
        0x15056DD4UL, 0x88F46DBAUL, 0x03A16125UL, 0x0564F0BDUL,
        0xC3EB9E15UL, 0x3C9057A2UL, 0x97271AECUL, 0xA93A072AUL,
        0x1B3F6D9BUL, 0x1E6321F5UL, 0xF59C66FBUL, 0x26DCF319UL,
        0x7533D928UL, 0xB155FDF5UL, 0x03563482UL, 0x8ABA3CBBUL,
        0x28517711UL, 0xC20AD9F8UL, 0xABCC5167UL, 0xCCAD925FUL,
        0x4DE81751UL, 0x3830DC8EUL, 0x379D5862UL, 0x9320F991UL,
        0xEA7A90C2UL, 0xFB3E7BCEUL, 0x5121CE64UL, 0x774FBE32UL,
        0xA8B6E37EUL, 0xC3293D46UL, 0x48DE5369UL, 0x6413E680UL,
        0xA2AE0810UL, 0xDD6DB224UL, 0x69852DFDUL, 0x09072166UL,
        0xB39A460AUL, 0x6445C0DDUL, 0x586CDECFUL, 0x1C20C8AEUL,
        0x5BBEF7DDUL, 0x1B588D40UL, 0xCCD2017FUL, 0x6BB4E3BBUL,
        0xDDA26A7EUL, 0x3A59FF45UL, 0x3E350A44UL, 0xBCB4CDD5UL,
        0x72EACEA8UL, 0xFA6484BBUL, 0x8D6612AEUL, 0xBF3C6F47UL,
        0xD29BE463UL, 0x542F5D9EUL, 0xAEC2771BUL, 0xF64E6370UL,
        0x740E0D8DUL, 0xE75B1357UL, 0xF8721671UL, 0xAF537D5DUL,
        0x4040CB08UL, 0x4EB4E2CCUL, 0x34D2466AUL, 0x0115AF84UL,
        0xE1B00428UL, 0x95983A1DUL, 0x06B89FB4UL, 0xCE6EA048UL,
        0x6F3F3B82UL, 0x3520AB82UL, 0x011A1D4BUL, 0x277227F8UL,
        0x611560B1UL, 0xE7933FDCUL, 0xBB3A792BUL, 0x344525BDUL,
        0xA08839E1UL, 0x51CE794BUL, 0x2F32C9B7UL, 0xA01FBAC9UL,
        0xE01CC87EUL, 0xBCC7D1F6UL, 0xCF0111C3UL, 0xA1E8AAC7UL,
        0x1A908749UL, 0xD44FBD9AUL, 0xD0DADECBUL, 0xD50ADA38UL,
        0x0339C32AUL, 0xC6913667UL, 0x8DF9317CUL, 0xE0B12B4FUL,
        0xF79E59B7UL, 0x43F5BB3AUL, 0xF2D519FFUL, 0x27D9459CUL,
        0xBF97222CUL, 0x15E6FC2AUL, 0x0F91FC71UL, 0x9B941525UL,
        0xFAE59361UL, 0xCEB69CEBUL, 0xC2A86459UL, 0x12BAA8D1UL,
        0xB6C1075EUL, 0xE3056A0CUL, 0x10D25065UL, 0xCB03A442UL,
        0xE0EC6E0EUL, 0x1698DB3BUL, 0x4C98A0BEUL, 0x3278E964UL,
        0x9F1F9532UL, 0xE0D392DFUL, 0xD3A0342BUL, 0x8971F21EUL,
        0x1B0A7441UL, 0x4BA3348CUL, 0xC5BE7120UL, 0xC37632D8UL,
        0xDF359F8DUL, 0x9B992F2EUL, 0xE60B6F47UL, 0x0FE3F11DUL,
        0xE54CDA54UL, 0x1EDAD891UL, 0xCE6279CFUL, 0xCD3E7E6FUL,
        0x1618B166UL, 0xFD2C1D05UL, 0x848FD2C5UL, 0xF6FB2299UL,
        0xF523F357UL, 0xA6327623UL, 0x93A83531UL, 0x56CCCD02UL,
        0xACF08162UL, 0x5A75EBB5UL, 0x6E163697UL, 0x88D273CCUL,
        0xDE966292UL, 0x81B949D0UL, 0x4C50901BUL, 0x71C65614UL,
        0xE6C6C7BDUL, 0x327A140AUL, 0x45E1D006UL, 0xC3F27B9AUL,
        0xC9AA53FDUL, 0x62A80F00UL, 0xBB25BFE2UL, 0x35BDD2F6UL,
        0x71126905UL, 0xB2040222UL, 0xB6CBCF7CUL, 0xCD769C2BUL,
        0x53113EC0UL, 0x1640E3D3UL, 0x38ABBD60UL, 0x2547ADF0UL,
        0xBA38209CUL, 0xF746CE76UL, 0x77AFA1C5UL, 0x20756060UL,
        0x85CBFE4EUL, 0x8AE88DD8UL, 0x7AAAF9B0UL, 0x4CF9AA7EUL,
        0x1948C25CUL, 0x02FB8A8CUL, 0x01C36AE4UL, 0xD6EBE1F9UL,
        0x90D4F869UL, 0xA65CDEA0UL, 0x3F09252DUL, 0xC208E69FUL,
        0xB74E6132UL, 0xCE77E25BUL, 0x578FDFE3UL, 0x3AC372E6UL  }
};

 /**
    Initialize the Blowfish block cipher
    @param key The symmetric key you wish to pass
    @param keylen The key length in bytes
    @param num_rounds The number of rounds desired (0 for default)
    @param skey The key in as scheduled by this function.
    @return CRYPT_OK if successful
 */
int blowfish_setup(const unsigned char *key, int keylen, int num_rounds,
                   symmetric_key *skey)
{
   ulong32 x, y, z, A;
   unsigned char B[8];

   LTC_ARGCHK(key != NULL);
   LTC_ARGCHK(skey != NULL);

   /* check key length */
   if (keylen < 8 || keylen > 56) {
      return CRYPT_INVALID_KEYSIZE;
   }

   /* check rounds */
   if (num_rounds != 0 && num_rounds != 16) {
      return CRYPT_INVALID_ROUNDS;
   }

   /* load in key bytes (Supplied by David Hopwood) */
   for (x = y = 0; x < 18; x++) {
       A = 0;
       for (z = 0; z < 4; z++) {
           A = (A << 8) | ((ulong32)key[y++] & 255);
           if (y == (ulong32)keylen) {
              y = 0;
           }
       }
       skey->blowfish.K[x] = ORIG_P[x] ^ A;
   }

   /* copy sboxes */
   for (x = 0; x < 4; x++) {
       for (y = 0; y < 256; y++) {
           skey->blowfish.S[x][y] = ORIG_S[x][y];
       }
   }

   /* encrypt K array */
   for (x = 0; x < 8; x++) {
       B[x] = 0;
   }

   for (x = 0; x < 18; x += 2) {
       /* encrypt it */
       blowfish_ecb_encrypt(B, B, skey);
       /* copy it */
       LOAD32H(skey->blowfish.K[x], &B[0]);
       LOAD32H(skey->blowfish.K[x+1], &B[4]);
   }

   /* encrypt S array */
   for (x = 0; x < 4; x++) {
       for (y = 0; y < 256; y += 2) {
          /* encrypt it */
          blowfish_ecb_encrypt(B, B, skey);
          /* copy it */
          LOAD32H(skey->blowfish.S[x][y], &B[0]);
          LOAD32H(skey->blowfish.S[x][y+1], &B[4]);
       }
   }

#ifdef LTC_CLEAN_STACK
   zeromem(B, sizeof(B));
#endif

   return CRYPT_OK;
}

#ifndef __GNUC__
#define F(x) ((S1[LTC_BYTE(x,3)] + S2[LTC_BYTE(x,2)]) ^ S3[LTC_BYTE(x,1)]) + S4[LTC_BYTE(x,0)]
#else
#define F(x) ((skey->blowfish.S[0][LTC_BYTE(x,3)] + skey->blowfish.S[1][LTC_BYTE(x,2)]) ^ skey->blowfish.S[2][LTC_BYTE(x,1)]) + skey->blowfish.S[3][LTC_BYTE(x,0)]
#endif

/**
  Encrypts a block of text with Blowfish
  @param pt The input plaintext (8 bytes)
  @param ct The output ciphertext (8 bytes)
  @param skey The key as scheduled
  @return CRYPT_OK if successful
*/
#ifdef LTC_CLEAN_STACK
static int _blowfish_ecb_encrypt(const unsigned char *pt, unsigned char *ct, const symmetric_key *skey)
#else
int blowfish_ecb_encrypt(const unsigned char *pt, unsigned char *ct, const symmetric_key *skey)
#endif
{
   ulong32 L, R;
   int r;
#ifndef __GNUC__
   const ulong32 *S1, *S2, *S3, *S4;
#endif

    LTC_ARGCHK(pt   != NULL);
    LTC_ARGCHK(ct   != NULL);
    LTC_ARGCHK(skey != NULL);

#ifndef __GNUC__
    S1 = skey->blowfish.S[0];
    S2 = skey->blowfish.S[1];
    S3 = skey->blowfish.S[2];
    S4 = skey->blowfish.S[3];
#endif

   /* load it */
   LOAD32H(L, &pt[0]);
   LOAD32H(R, &pt[4]);

   /* do 16 rounds */
   for (r = 0; r < 16; ) {
      L ^= skey->blowfish.K[r++];  R ^= F(L);
      R ^= skey->blowfish.K[r++];  L ^= F(R);
      L ^= skey->blowfish.K[r++];  R ^= F(L);
      R ^= skey->blowfish.K[r++];  L ^= F(R);
   }

   /* last keying */
   R ^= skey->blowfish.K[17];
   L ^= skey->blowfish.K[16];

   /* store */
   STORE32H(R, &ct[0]);
   STORE32H(L, &ct[4]);

   return CRYPT_OK;
}

#ifdef LTC_CLEAN_STACK
int blowfish_ecb_encrypt(const unsigned char *pt, unsigned char *ct, const symmetric_key *skey)
{
    int err = _blowfish_ecb_encrypt(pt, ct, skey);
    burn_stack(sizeof(ulong32) * 2 + sizeof(int));
    return err;
}
#endif

/**
  Decrypts a block of text with Blowfish
  @param ct The input ciphertext (8 bytes)
  @param pt The output plaintext (8 bytes)
  @param skey The key as scheduled
  @return CRYPT_OK if successful
*/
#ifdef LTC_CLEAN_STACK
static int _blowfish_ecb_decrypt(const unsigned char *ct, unsigned char *pt, const symmetric_key *skey)
#else
int blowfish_ecb_decrypt(const unsigned char *ct, unsigned char *pt, const symmetric_key *skey)
#endif
{
   ulong32 L, R;
   int r;
#ifndef __GNUC__
   const ulong32 *S1, *S2, *S3, *S4;
#endif

    LTC_ARGCHK(pt   != NULL);
    LTC_ARGCHK(ct   != NULL);
    LTC_ARGCHK(skey != NULL);

#ifndef __GNUC__
    S1 = skey->blowfish.S[0];
    S2 = skey->blowfish.S[1];
    S3 = skey->blowfish.S[2];
    S4 = skey->blowfish.S[3];
#endif

   /* load it */
   LOAD32H(R, &ct[0]);
   LOAD32H(L, &ct[4]);

   /* undo last keying */
   R ^= skey->blowfish.K[17];
   L ^= skey->blowfish.K[16];

   /* do 16 rounds */
   for (r = 15; r > 0; ) {
      L ^= F(R); R ^= skey->blowfish.K[r--];
      R ^= F(L); L ^= skey->blowfish.K[r--];
      L ^= F(R); R ^= skey->blowfish.K[r--];
      R ^= F(L); L ^= skey->blowfish.K[r--];
   }

   /* store */
   STORE32H(L, &pt[0]);
   STORE32H(R, &pt[4]);
   return CRYPT_OK;
}

#ifdef LTC_CLEAN_STACK
int blowfish_ecb_decrypt(const unsigned char *ct, unsigned char *pt, const symmetric_key *skey)
{
    int err = _blowfish_ecb_decrypt(ct, pt, skey);
    burn_stack(sizeof(ulong32) * 2 + sizeof(int));
    return err;
}
#endif


/**
  Performs a self-test of the Blowfish block cipher
  @return CRYPT_OK if functional, CRYPT_NOP if self-test has been disabled
*/
int blowfish_test(void)
{
 #ifndef LTC_TEST
    return CRYPT_NOP;
 #else
   int err;
   symmetric_key key;
   static const struct {
          unsigned char key[8], pt[8], ct[8];
   } tests[] = {
       {
           { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
           { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
           { 0x4E, 0xF9, 0x97, 0x45, 0x61, 0x98, 0xDD, 0x78}
       },
       {
           { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
           { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
           { 0x51, 0x86, 0x6F, 0xD5, 0xB8, 0x5E, 0xCB, 0x8A}
       },
       {
           { 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
           { 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01},
           { 0x7D, 0x85, 0x6F, 0x9A, 0x61, 0x30, 0x63, 0xF2}
       }
   };
   unsigned char tmp[2][8];
   int x, y;

   for (x = 0; x < (int)(sizeof(tests) / sizeof(tests[0])); x++) {
      /* setup key */
      if ((err = blowfish_setup(tests[x].key, 8, 16, &key)) != CRYPT_OK) {
         return err;
      }

      /* encrypt and decrypt */
      blowfish_ecb_encrypt(tests[x].pt, tmp[0], &key);
      blowfish_ecb_decrypt(tmp[0], tmp[1], &key);

      /* compare */
      if ((compare_testvector(tmp[0], 8, tests[x].ct, 8, "Blowfish Encrypt", x) != 0) ||
            (compare_testvector(tmp[1], 8, tests[x].pt, 8, "Blowfish Decrypt", x) != 0)) {
         return CRYPT_FAIL_TESTVECTOR;
      }

      /* now see if we can encrypt all zero bytes 1000 times, decrypt and come back where we started */
      for (y = 0; y < 8; y++) tmp[0][y] = 0;
      for (y = 0; y < 1000; y++) blowfish_ecb_encrypt(tmp[0], tmp[0], &key);
      for (y = 0; y < 1000; y++) blowfish_ecb_decrypt(tmp[0], tmp[0], &key);
      for (y = 0; y < 8; y++) if (tmp[0][y] != 0) return CRYPT_FAIL_TESTVECTOR;
   }
   return CRYPT_OK;
 #endif
}

/** Terminate the context
   @param skey    The scheduled key
*/
void blowfish_done(symmetric_key *skey)
{
  LTC_UNUSED_PARAM(skey);
}

/**
  Gets suitable key size
  @param keysize [in/out] The length of the recommended key (in bytes).  This function will store the suitable size back in this variable.
  @return CRYPT_OK if the input key size is acceptable.
*/
int blowfish_keysize(int *keysize)
{
   LTC_ARGCHK(keysize != NULL);

   if (*keysize < 8) {
      return CRYPT_INVALID_KEYSIZE;
   }
   if (*keysize > 56) {
      *keysize = 56;
   }
   return CRYPT_OK;
}

#endif


/* ref:         HEAD -> develop */
/* git commit:  01c455c3d5f781312de84594a11e102a20d5b959 */
/* commit time: 2018-12-17 15:44:02 +0100 */
