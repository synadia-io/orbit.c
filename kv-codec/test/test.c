// Copyright 2026 Synadia Communications Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// kv-codec test suite.
//
// The codec unit tests run without a server; the KV* tests spawn a
// JetStream-enabled nats-server (must be on PATH, or set NATS_TEST_SERVER_EXE).
//
// Run a single test:
//   ./kv_codec_testsuite Base64KeyRoundtrip
// Run all tests:
//   ctest --test-dir build -R '^kvc_'

#include "kvcodecp.h" // internal dispatch helpers, exercised directly

#include "test.h"

// KV setup macros: JetStream-enabled server + a bucket with history.

#define KV_SETUP                                    \
    kvStore *kv = NULL;                             \
    kvConfig kvc;                                   \
                                                    \
    JS_SETUP;                                       \
                                                    \
    test("Create bucket: ");                        \
    kvConfig_Init(&kvc);                            \
    kvc.Bucket  = "TEST";                           \
    kvc.History = 5;                                \
    s           = js_CreateKeyValue(&kv, js, &kvc); \
    testCond(s == NATS_OK)

#define KV_TEARDOWN      \
    kvStore_Destroy(kv); \
    JS_TEARDOWN

// Custom codec helpers used by the unit tests.

// Prefix codec: encode prepends "<prefix>:", decode strips it (and errors if
// the prefix is missing)
static natsStatus
_prefixEncode(char **out, const char *in, void *closure)
{
    const char *prefix = (const char *)closure;
    char       *buf    = (char *)kvCodec_AllocBuf(strlen(prefix) + 1 + strlen(in) + 1);

    if (buf == NULL)
        return NATS_NO_MEMORY;
    sprintf(buf, "%s:%s", prefix, in);
    *out = buf;
    return NATS_OK;
}

static natsStatus
_prefixDecode(char **out, const char *in, void *closure)
{
    const char *prefix = (const char *)closure;
    size_t      plen   = strlen(prefix);
    char       *buf;

    if ((strncmp(in, prefix, plen) != 0) || (in[plen] != ':'))
        return NATS_ERR;
    buf = (char *)kvCodec_AllocBuf(strlen(in) - plen);
    if (buf == NULL)
        return NATS_NO_MEMORY;
    strcpy(buf, in + plen + 1);
    *out = buf;
    return NATS_OK;
}

// Value codec that produces an empty output as (*out = NULL, *outLen = 0) —
// legal per the contract, since kvCodec_AllocBuf(0) may return NULL.
static natsStatus
_emptyValueTransform(void **out, int *outLen, const void *in, int inLen, void *closure)
{
    (void)in;
    (void)inLen;
    (void)closure;
    *out    = NULL;
    *outLen = 0;
    return NATS_OK;
}

// Value codec that prepends its closure string to the input.
static natsStatus
_prefixValueTransform(void **out, int *outLen, const void *in, int inLen, void *closure)
{
    const char *prefix = (const char *)closure;
    size_t      plen   = strlen(prefix);
    char       *buf    = (char *)kvCodec_AllocBuf(plen + (size_t)inLen + 1);

    if (buf == NULL)
        return NATS_NO_MEMORY;
    memcpy(buf, prefix, plen);
    if (inLen > 0)
        memcpy(buf + plen, in, (size_t)inLen);
    *out    = buf;
    *outLen = (int)plen + inLen;
    return NATS_OK;
}

// Error codec: always fails with a recognizable custom status.
#define ERR_CODEC_STATUS NATS_MISMATCH

static natsStatus
_errTransform(char **out, const char *in, void *closure)
{
    (void)out;
    (void)in;
    (void)closure;
    return ERR_CODEC_STATUS;
}

// Key codec that (illegally) returns NATS_OK with *out left NULL — the
// kvKeyTransformF contract requires a non-NULL string.
static natsStatus
_nullKeyTransform(char **out, const char *in, void *closure)
{
    (void)in;
    (void)closure;
    *out = NULL;
    return NATS_OK;
}

// Uppercase key codec (not filterable): used to prove wildcard rejection.
static natsStatus
_upperEncode(char **out, const char *in, void *closure)
{
    char  *buf = (char *)kvCodec_AllocBuf(strlen(in) + 1);
    size_t i;

    (void)closure;
    if (buf == NULL)
        return NATS_NO_MEMORY;
    for (i = 0; in[i] != '\0'; i++)
        buf[i] = (char)((in[i] >= 'a' && in[i] <= 'z') ? in[i] - 32 : in[i]);
    buf[i] = '\0';
    *out   = buf;
    return NATS_OK;
}

static natsStatus
_lowerDecode(char **out, const char *in, void *closure)
{
    char  *buf = (char *)kvCodec_AllocBuf(strlen(in) + 1);
    size_t i;

    (void)closure;
    if (buf == NULL)
        return NATS_NO_MEMORY;
    for (i = 0; in[i] != '\0'; i++)
        buf[i] = (char)((in[i] >= 'A' && in[i] <= 'Z') ? in[i] + 32 : in[i]);
    buf[i] = '\0';
    *out   = buf;
    return NATS_OK;
}

static int _destroyCount = 0;

static void
_countDestroy(void *closure)
{
    (void)closure;
    _destroyCount++;
}

// Checks one key transform against an expected result, freeing the output.
static bool
_checkTransform(natsStatus s, char *got, const char *expected)
{
    bool ok = (s == NATS_OK) && (got != NULL) && (strcmp(got, expected) == 0);

    if ((s == NATS_OK) && (got != NULL) && !ok)
        printf("(got \"%s\", want \"%s\") ", got, expected);
    kvCodec_FreeBuf(got);
    return ok;
}

//=============================================================================
// Codec unit tests — no server needed.
//=============================================================================

void
test_Base64KeyRoundtrip(void)
{
    kvKeyCodec *kc = NULL;
    natsStatus  s;
    char       *enc      = NULL;
    char       *dec      = NULL;
    const char *key      = "test.key.with.special.chars!@#$%^&*()";
    const char *expected = "dGVzdA.a2V5.d2l0aA.c3BlY2lhbA.Y2hhcnMhQCMkJV4mKigp";

    test("Create codec: ");
    s = kvKeyCodec_Base64(&kc);
    testCond(s == NATS_OK);

    test("Encode key (per-token, URL-safe, unpadded): ");
    s = kvcodec_encodeKey(&enc, kc, key);
    testCond(_checkTransform(s, enc, expected));

    test("Decode key round-trips: ");
    s = kvcodec_decodeKey(&dec, kc, expected);
    testCond(_checkTransform(s, dec, key));

    // Note: keys with empty tokens encode fine but are rejected by NATS KV
    // (consecutive dots); the transform itself must still match Go.
    test("Empty tokens transform (unstorable as KV keys): ");
    s = kvcodec_encodeKey(&enc, kc, "a..b");
    testCond(_checkTransform(s, enc, "YQ..Yg"));

    kvKeyCodec_Destroy(kc);
}

void
test_Base64ValueRoundtrip(void)
{
    kvValueCodec       *vc = NULL;
    natsStatus          s;
    void               *enc      = NULL;
    void               *dec      = NULL;
    int                 encLen   = 0;
    int                 decLen   = 0;
    const unsigned char binary[] = { 0x00, 0x01, 0xFF, 'a', 0x00, 0x7E };

    test("Create codec: ");
    s = kvValueCodec_Base64(&vc);
    testCond(s == NATS_OK);

    test("Encode known vector: ");
    s = kvcodec_encodeValue(&enc, &encLen, vc, "hello", 5);
    testCond((s == NATS_OK) && (encLen == 7) && (memcmp(enc, "aGVsbG8", 7) == 0));
    kvCodec_FreeBuf(enc);
    enc = NULL;

    test("Binary value (embedded NULs) round-trips: ");
    s = kvcodec_encodeValue(&enc, &encLen, vc, binary, (int)sizeof(binary));
    if (s == NATS_OK)
        s = kvcodec_decodeValue(&dec, &decLen, vc, enc, encLen);
    testCond((s == NATS_OK) && (decLen == (int)sizeof(binary)) && (memcmp(dec, binary, sizeof(binary)) == 0));
    kvCodec_FreeBuf(enc);
    kvCodec_FreeBuf(dec);
    enc = NULL;
    dec = NULL;

    test("Empty value round-trips: ");
    s = kvcodec_encodeValue(&enc, &encLen, vc, NULL, 0);
    if (s == NATS_OK)
        s = kvcodec_decodeValue(&dec, &decLen, vc, enc, encLen);
    testCond((s == NATS_OK) && (encLen == 0) && (decLen == 0));
    kvCodec_FreeBuf(enc);
    kvCodec_FreeBuf(dec);

    kvValueCodec_Destroy(vc);
}

void
test_Base64DecodeInvalid(void)
{
    natsStatus  s;
    void       *out    = NULL;
    int         outLen = 0;
    kvKeyCodec *kc     = NULL;
    char       *dec    = NULL;

    test("Padding is rejected: ");
    s = kvcodec_base64RawURLDecode(&out, &outLen, "dGVzdA==", 8);
    testCond(s == NATS_ERR);

    test("Invalid character is rejected: ");
    s = kvcodec_base64RawURLDecode(&out, &outLen, "ab+c", 4);
    testCond(s == NATS_ERR);

    test("Impossible length (n%4==1) is rejected: ");
    s = kvcodec_base64RawURLDecode(&out, &outLen, "abcde", 5);
    testCond(s == NATS_ERR);

    test("Bad token fails key decode: ");
    s = kvKeyCodec_Base64(&kc);
    if (s == NATS_OK)
        s = kvcodec_decodeKey(&dec, kc, "dGVzdA.!!!!");
    testCond(s == NATS_ERR);

    test("Non-canonical trailing bits accepted (Go parity): ");
    // "QR" = Q(16),R(17): low 4 bits of R are non-zero; Go's non-Strict
    // RawURLEncoding decodes it to "A", and so must we.
    s = kvcodec_base64RawURLDecode(&out, &outLen, "QR", 2);
    testCond((s == NATS_OK) && (outLen == 1) && (((const char *)out)[0] == 'A'));
    kvCodec_FreeBuf(out);
    out = NULL;

    test("Oversized encode input rejected (int overflow guard): ");
    // The guard fires before the input is read, so a small buffer is safe.
    {
        char *encOut    = NULL;
        int   encOutLen = 0;

        s = kvcodec_base64RawURLEncode(&encOut, &encOutLen, "x", 1700000000);
        testCond(s == NATS_INVALID_ARG);
    }

    kvKeyCodec_Destroy(kc);
}

void
test_PathCodecTable(void)
{
    static const struct
    {
        const char *input;
        const char *encoded;
        const char *decoded;
    } rows[] = {
        { "/foo/bar", "_root_.foo.bar", "/foo/bar" },
        { "foo/bar", "foo.bar", "foo/bar" },
        { "/foo/bar/baz/qux", "_root_.foo.bar.baz.qux", "/foo/bar/baz/qux" },
        { "/foo", "_root_.foo", "/foo" },
        { "foo/bar/", "foo.bar", "foo/bar" },
        { "/", "_root_", "/" },
        { "/foo/bar/", "_root_.foo.bar", "/foo/bar" },
    };
    kvKeyCodec *kc = NULL;
    natsStatus  s;
    int         i;

    test("Create codec: ");
    s = kvKeyCodec_Path(&kc);
    testCond(s == NATS_OK);

    for (i = 0; i < (int)(sizeof(rows) / sizeof(rows[0])); i++)
    {
        char  buf[128];
        char *enc = NULL;
        char *dec = NULL;
        bool  ok;

        snprintf(buf, sizeof(buf), "Row \"%s\": ", rows[i].input);
        test(buf);
        s  = kvcodec_encodeKey(&enc, kc, rows[i].input);
        ok = (s == NATS_OK) && (strcmp(enc, rows[i].encoded) == 0);
        if (ok)
        {
            s  = kvcodec_decodeKey(&dec, kc, enc);
            ok = (s == NATS_OK) && (strcmp(dec, rows[i].decoded) == 0);
        }
        kvCodec_FreeBuf(enc);
        kvCodec_FreeBuf(dec);
        testCond(ok);
    }

    kvKeyCodec_Destroy(kc);
}

void
test_NoOpCodec(void)
{
    kvKeyCodec   *kc = NULL;
    kvValueCodec *vc = NULL;
    natsStatus    s;
    char         *out     = NULL;
    void         *vout    = NULL;
    int           voutLen = 0;

    test("Create codecs: ");
    s = kvKeyCodec_NoOp(&kc);
    if (s == NATS_OK)
        s = kvValueCodec_NoOp(&vc);
    testCond(s == NATS_OK);

    test("Key passthrough: ");
    s = kvcodec_encodeKey(&out, kc, "foo.bar");
    testCond(_checkTransform(s, out, "foo.bar"));

    test("Filterable (wildcards pass through): ");
    s = kvcodec_encodeFilter(&out, kc, "foo.*.>");
    testCond(_checkTransform(s, out, "foo.*.>"));

    test("Value passthrough: ");
    s = kvcodec_encodeValue(&vout, &voutLen, vc, "abc", 3);
    testCond((s == NATS_OK) && (voutLen == 3) && (memcmp(vout, "abc", 3) == 0));
    kvCodec_FreeBuf(vout);

    kvKeyCodec_Destroy(kc);
    kvValueCodec_Destroy(vc);
}

void
test_EncodeFilterBase64(void)
{
    static const struct
    {
        const char *filter;
        const char *expected;
    } rows[] = {
        { "user.123", "dXNlcg.MTIz" },
        { "user.*", "dXNlcg.*" },
        { "user.>", "dXNlcg.>" },
        { "app.*.config.>", "YXBw.*.Y29uZmln.>" },
    };
    kvKeyCodec *kc = NULL;
    natsStatus  s;
    int         i;

    test("Create codec: ");
    s = kvKeyCodec_Base64(&kc);
    testCond(s == NATS_OK);

    for (i = 0; i < (int)(sizeof(rows) / sizeof(rows[0])); i++)
    {
        char  buf[128];
        char *enc = NULL;

        snprintf(buf, sizeof(buf), "Filter \"%s\": ", rows[i].filter);
        test(buf);
        s = kvcodec_encodeFilter(&enc, kc, rows[i].filter);
        testCond(_checkTransform(s, enc, rows[i].expected));
    }

    test("Non-token wildcard is encoded literally: ");
    {
        char *enc = NULL;

        s = kvcodec_encodeFilter(&enc, kc, "foo*bar");
        // "foo*bar" is not a standalone wildcard token: base64 of the whole token.
        testCond(_checkTransform(s, enc, "Zm9vKmJhcg"));
    }

    kvKeyCodec_Destroy(kc);
}

void
test_EncodeFilterPath(void)
{
    static const struct
    {
        const char *filter;
        const char *expected;
    } rows[] = {
        { "/user/*", "_root_.user.*" },
        { "/app/*/config/>", "_root_.app.*.config.>" },
        { "user/*", "user.*" },
    };
    kvKeyCodec *kc = NULL;
    natsStatus  s;
    int         i;

    test("Create codec: ");
    s = kvKeyCodec_Path(&kc);
    testCond(s == NATS_OK);

    for (i = 0; i < (int)(sizeof(rows) / sizeof(rows[0])); i++)
    {
        char  buf[128];
        char *enc = NULL;

        snprintf(buf, sizeof(buf), "Filter \"%s\": ", rows[i].filter);
        test(buf);
        s = kvcodec_encodeFilter(&enc, kc, rows[i].filter);
        testCond(_checkTransform(s, enc, rows[i].expected));
    }

    kvKeyCodec_Destroy(kc);
}

void
test_CustomCodecArgs(void)
{
    kvKeyCodec   *kc = NULL;
    kvValueCodec *vc = NULL;
    natsStatus    s;
    char         *out = NULL;

    test("NULL out-param rejected: ");
    s = kvKeyCodec_New(NULL, _prefixEncode, _prefixDecode, NULL, NULL, NULL);
    testCond(s == NATS_INVALID_ARG);

    test("NULL encode rejected: ");
    s = kvKeyCodec_New(&kc, NULL, _prefixDecode, NULL, NULL, NULL);
    testCond(s == NATS_INVALID_ARG);

    test("NULL decode rejected: ");
    s = kvKeyCodec_New(&kc, _prefixEncode, NULL, NULL, NULL, NULL);
    testCond(s == NATS_INVALID_ARG);

    test("Value codec NULL callbacks rejected: ");
    s = kvValueCodec_New(&vc, NULL, NULL, NULL, NULL);
    testCond(s == NATS_INVALID_ARG);

    test("Custom codec works through dispatch: ");
    s = kvKeyCodec_New(&kc, _prefixEncode, _prefixDecode, NULL, _countDestroy, (void *)"P");
    if (s == NATS_OK)
        s = kvcodec_encodeKey(&out, kc, "key");
    testCond(_checkTransform(s, out, "P:key"));

    test("Non-filterable codec rejects wildcard patterns: ");
    s = kvcodec_encodeFilter(&out, kc, "foo.*");
    testCond(s == NATS_INVALID_SUBJECT);

    test("Non-filterable codec still encodes exact patterns: ");
    s = kvcodec_encodeFilter(&out, kc, "foo.bar");
    testCond(_checkTransform(s, out, "P:foo.bar"));

    test("Destroy invokes the callback exactly once: ");
    _destroyCount = 0;
    kvKeyCodec_Destroy(kc);
    testCond(_destroyCount == 1);

    test("AllocBuf/FreeBuf round-trip: ");
    out = (char *)kvCodec_AllocBuf(4);
    if (out != NULL)
        strcpy(out, "abc");
    testCond((out != NULL) && (strcmp(out, "abc") == 0));
    kvCodec_FreeBuf(out);

    test("FreeBuf(NULL) is a no-op: ");
    kvCodec_FreeBuf(NULL);
    testCond(true);
}

void
test_ChainArgs(void)
{
    kvKeyCodec *noop       = NULL;
    kvKeyCodec *chain      = NULL;
    kvKeyCodec *members[2] = { NULL, NULL };
    natsStatus  s;

    test("Setup: ");
    s = kvKeyCodec_NoOp(&noop);
    testCond(s == NATS_OK);

    test("Empty chain rejected: ");
    members[0] = noop;
    s          = kvKeyCodec_Chain(&chain, members, 0);
    testCond(s == NATS_INVALID_ARG);

    test("NULL codecs array rejected: ");
    s = kvKeyCodec_Chain(&chain, NULL, 1);
    testCond(s == NATS_INVALID_ARG);

    test("NULL member rejected: ");
    members[0] = noop;
    members[1] = NULL;
    s          = kvKeyCodec_Chain(&chain, members, 2);
    testCond(s == NATS_INVALID_ARG);

    kvKeyCodec_Destroy(noop);
}

void
test_ChainOrder(void)
{
    kvKeyCodec *codecA = NULL;
    kvKeyCodec *codecB = NULL;
    kvKeyCodec *chain  = NULL;
    kvKeyCodec *members[2];
    natsStatus  s;
    char       *out = NULL;

    test("Setup prefix codecs: ");
    s = kvKeyCodec_New(&codecA, _prefixEncode, _prefixDecode, NULL, NULL, (void *)"A");
    if (s == NATS_OK)
        s = kvKeyCodec_New(&codecB, _prefixEncode, _prefixDecode, NULL, NULL, (void *)"B");
    testCond(s == NATS_OK);

    test("Create chain [A, B]: ");
    members[0] = codecA;
    members[1] = codecB;
    s          = kvKeyCodec_Chain(&chain, members, 2);
    testCond(s == NATS_OK);

    test("Encode applies first-to-last (B:A:test): ");
    s = kvcodec_encodeKey(&out, chain, "test");
    testCond(_checkTransform(s, out, "B:A:test"));

    test("Decode reverses (last-to-first): ");
    s = kvcodec_decodeKey(&out, chain, "B:A:test");
    testCond(_checkTransform(s, out, "test"));

    kvKeyCodec_Destroy(chain);
    kvKeyCodec_Destroy(codecA);
    kvKeyCodec_Destroy(codecB);
}

void
test_ChainRoundtrip(void)
{
    kvKeyCodec *path  = NULL;
    kvKeyCodec *b64   = NULL;
    kvKeyCodec *chain = NULL;
    kvKeyCodec *members[2];
    natsStatus  s;
    char       *enc = NULL;
    char       *dec = NULL;
    const char *key = "/config/app/database/host";

    test("Create chain [path, base64]: ");
    s = kvKeyCodec_Path(&path);
    if (s == NATS_OK)
        s = kvKeyCodec_Base64(&b64);
    members[0] = path;
    members[1] = b64;
    if (s == NATS_OK)
        s = kvKeyCodec_Chain(&chain, members, 2);
    testCond(s == NATS_OK);

    test("Encode changes the key: ");
    s = kvcodec_encodeKey(&enc, chain, key);
    testCond((s == NATS_OK) && (strcmp(enc, key) != 0));

    test("Decode round-trips: ");
    s = kvcodec_decodeKey(&dec, chain, enc);
    testCond(_checkTransform(s, dec, key));
    kvCodec_FreeBuf(enc);

    kvKeyCodec_Destroy(chain);
    kvKeyCodec_Destroy(path);
    kvKeyCodec_Destroy(b64);
}

void
test_ChainFilterable(void)
{
    kvKeyCodec *path   = NULL;
    kvKeyCodec *b64    = NULL;
    kvKeyCodec *custom = NULL;
    kvKeyCodec *chain  = NULL;
    kvKeyCodec *members[2];
    natsStatus  s;
    char       *out = NULL;

    test("Setup: ");
    s = kvKeyCodec_Path(&path);
    if (s == NATS_OK)
        s = kvKeyCodec_Base64(&b64);
    if (s == NATS_OK) // not filterable: no filter callback
        s = kvKeyCodec_New(&custom, _prefixEncode, _prefixDecode, NULL, NULL, (void *)"P");
    testCond(s == NATS_OK);

    test("All-filterable chain encodes wildcards: ");
    members[0] = path;
    members[1] = b64;
    s          = kvKeyCodec_Chain(&chain, members, 2);
    if (s == NATS_OK)
        s = kvcodec_encodeFilter(&out, chain, "/user/*");
    testCond(_checkTransform(s, out, "X3Jvb3Rf.dXNlcg.*"));
    kvKeyCodec_Destroy(chain);
    chain = NULL;

    test("Chain with non-filterable member rejects wildcards: ");
    members[0] = b64;
    members[1] = custom;
    s          = kvKeyCodec_Chain(&chain, members, 2);
    if (s == NATS_OK)
        s = kvcodec_encodeFilter(&out, chain, "user.*");
    testCond(s == NATS_INVALID_SUBJECT);

    kvKeyCodec_Destroy(chain);
    kvKeyCodec_Destroy(custom);
    kvKeyCodec_Destroy(b64);
    kvKeyCodec_Destroy(path);
}

void
test_ChainErrorPropagation(void)
{
    kvKeyCodec *noop  = NULL;
    kvKeyCodec *bad   = NULL;
    kvKeyCodec *chain = NULL;
    kvKeyCodec *members[2];
    natsStatus  s;
    char       *out = NULL;

    test("Setup: ");
    s = kvKeyCodec_NoOp(&noop);
    if (s == NATS_OK)
        s = kvKeyCodec_New(&bad, _errTransform, _errTransform, NULL, NULL, NULL);
    testCond(s == NATS_OK);

    test("Member's encode error propagates verbatim: ");
    members[0] = noop;
    members[1] = bad;
    s          = kvKeyCodec_Chain(&chain, members, 2);
    if (s == NATS_OK)
        s = kvcodec_encodeKey(&out, chain, "test");
    testCond(s == ERR_CODEC_STATUS);

    test("Member's decode error propagates verbatim: ");
    s = kvcodec_decodeKey(&out, chain, "test");
    testCond(s == ERR_CODEC_STATUS);

    kvKeyCodec_Destroy(chain);
    kvKeyCodec_Destroy(bad);
    kvKeyCodec_Destroy(noop);
}

void
test_ChainEmptyIntermediate(void)
{
    kvValueCodec *empties  = NULL;
    kvValueCodec *prefixer = NULL;
    kvValueCodec *chain    = NULL;
    kvValueCodec *members[2];
    natsStatus    s;
    void         *out    = NULL;
    int           outLen = -1;

    test("Setup: ");
    s = kvValueCodec_New(&empties, _emptyValueTransform, _emptyValueTransform, NULL, NULL);
    if (s == NATS_OK)
        s = kvValueCodec_New(&prefixer, _prefixValueTransform, _prefixValueTransform,
                             NULL, (void *)"B:");
    testCond(s == NATS_OK);

    // A NULL/0 intermediate result must feed the next stage as empty input,
    // not fall back to the chain's original input.
    test("Empty intermediate output is not confused with the original input: ");
    members[0] = empties;
    members[1] = prefixer;
    s          = kvValueCodec_Chain(&chain, members, 2);
    if (s == NATS_OK)
        s = kvcodec_encodeValue(&out, &outLen, chain, "xyz", 3);
    testCond((s == NATS_OK) && (outLen == 2) && (memcmp(out, "B:", 2) == 0));
    kvCodec_FreeBuf(out);

    kvValueCodec_Destroy(chain);
    kvValueCodec_Destroy(prefixer);
    kvValueCodec_Destroy(empties);
}

void
test_ChainNullIntermediate(void)
{
    kvKeyCodec *nullKc = NULL;
    kvKeyCodec *b64    = NULL;
    kvKeyCodec *chain  = NULL;
    kvKeyCodec *members[2];
    natsStatus  s;
    char       *out = NULL;

    test("Setup: ");
    s = kvKeyCodec_New(&nullKc, _nullKeyTransform, _nullKeyTransform, NULL, NULL, NULL);
    if (s == NATS_OK)
        s = kvKeyCodec_Base64(&b64);
    testCond(s == NATS_OK);

    // A key stage returning NATS_OK with *out == NULL violates the
    // kvKeyTransformF contract; the chain must fail instead of handing NULL
    // to the next member.
    test("NULL key intermediate fails cleanly: ");
    members[0] = nullKc;
    members[1] = b64;
    s          = kvKeyCodec_Chain(&chain, members, 2);
    if (s == NATS_OK)
        s = kvcodec_encodeKey(&out, chain, "test");
    testCond((s == NATS_ERR) && (out == NULL));
    kvKeyCodec_Destroy(chain);
    chain = NULL;

    test("NULL output from the last member fails too: ");
    members[0] = b64;
    members[1] = nullKc;
    s          = kvKeyCodec_Chain(&chain, members, 2);
    if (s == NATS_OK)
        s = kvcodec_encodeKey(&out, chain, "test");
    testCond((s == NATS_ERR) && (out == NULL));

    kvKeyCodec_Destroy(chain);
    kvKeyCodec_Destroy(b64);
    kvKeyCodec_Destroy(nullKc);
}

//=============================================================================
// Integration tests — require a JetStream-enabled nats-server.
//=============================================================================

void
test_KVBase64(void)
{
    kvKeyCodec      *keyCodec   = NULL;
    kvValueCodec    *valueCodec = NULL;
    kvCodec         *c          = NULL;
    kvCodecEntry    *entry      = NULL;
    kvEntry         *raw        = NULL;
    kvCodecEntryList history    = { NULL, 0 };
    uint64_t         rev        = 0;
    const char      *key        = "Acme Inc.contact"; // space would be invalid raw, dot splits tokens

    KV_SETUP;

    test("Create codec wrapper: ");
    s = kvKeyCodec_Base64(&keyCodec);
    if (s == NATS_OK)
        s = kvValueCodec_Base64(&valueCodec);
    if (s == NATS_OK)
        s = kvCodec_New(&c, kv, keyCodec, valueCodec);
    testCond(s == NATS_OK);

    test("PutString: ");
    s = kvCodec_PutString(&rev, c, key, "v1");
    testCond((s == NATS_OK) && (rev == 1));

    test("Get returns the original key and value: ");
    s = kvCodec_Get(&entry, c, key);
    testCond((s == NATS_OK) && (strcmp(kvCodecEntry_Key(entry), key) == 0) && (strcmp(kvCodecEntry_ValueString(entry), "v1") == 0) && (kvCodecEntry_ValueLen(entry) == 2) && (kvCodecEntry_Revision(entry) == 1) && (kvCodecEntry_Operation(entry) == kvOp_Put));
    kvCodecEntry_Destroy(entry);
    entry = NULL;

    test("Raw store holds the encoded key and value: ");
    s = kvStore_Get(&raw, kv, "QWNtZSBJbmM.Y29udGFjdA");
    testCond((s == NATS_OK) && (strcmp(kvEntry_ValueString(raw), "djE") == 0));
    kvEntry_Destroy(raw);

    test("Update with revision: ");
    s = kvCodec_UpdateString(&rev, c, key, "v2", rev);
    testCond((s == NATS_OK) && (rev == 2));

    test("GetRevision fetches the old value: ");
    s = kvCodec_GetRevision(&entry, c, key, 1);
    testCond((s == NATS_OK) && (strcmp(kvCodecEntry_ValueString(entry), "v1") == 0));
    kvCodecEntry_Destroy(entry);
    entry = NULL;

    test("Empty value round-trips as non-NULL, zero-length: ");
    s = kvCodec_PutString(NULL, c, "empty.key", "");
    if (s == NATS_OK)
        s = kvCodec_Get(&entry, c, "empty.key");
    testCond((s == NATS_OK) && (kvCodecEntry_Value(entry) != NULL) && (kvCodecEntry_ValueLen(entry) == 0) && (strcmp(kvCodecEntry_ValueString(entry), "") == 0));
    kvCodecEntry_Destroy(entry);
    entry = NULL;

    test("Create fails on existing key: ");
    s = kvCodec_CreateString(NULL, c, key, "nope");
    testCond(s != NATS_OK);

    test("History reports the original key, newest last: ");
    s = kvCodec_History(&history, c, key, NULL);
    testCond((s == NATS_OK) && (history.Count == 2) && (strcmp(kvCodecEntry_Key(history.Entries[0]), key) == 0) && (strcmp(kvCodecEntry_ValueString(history.Entries[1]), "v2") == 0));
    kvCodecEntryList_Destroy(&history);

    test("Delete: ");
    s = kvCodec_Delete(c, key);
    testCond(s == NATS_OK);

    test("Get after delete: ");
    s = kvCodec_Get(&entry, c, key);
    testCond(s == NATS_NOT_FOUND);

    kvCodec_Destroy(c);
    kvKeyCodec_Destroy(keyCodec);
    kvValueCodec_Destroy(valueCodec);
    KV_TEARDOWN;
}

void
test_KVPath(void)
{
    kvKeyCodec   *keyCodec = NULL;
    kvCodec      *c        = NULL;
    kvCodecEntry *entry    = NULL;
    kvEntry      *raw      = NULL;
    const char   *key      = "/config/app/database";

    KV_SETUP;

    test("Create codec wrapper (keys only): ");
    s = kvKeyCodec_Path(&keyCodec);
    if (s == NATS_OK)
        s = kvCodec_New(&c, kv, keyCodec, NULL);
    testCond(s == NATS_OK);

    test("PutString with a path key: ");
    s = kvCodec_PutString(NULL, c, key, "cfg");
    testCond(s == NATS_OK);

    test("Get returns the path key and the raw (uncopied) value: ");
    s = kvCodec_Get(&entry, c, key);
    testCond((s == NATS_OK) && (strcmp(kvCodecEntry_Key(entry), key) == 0) && (kvCodecEntry_Value(entry) != NULL) && (kvCodecEntry_ValueLen(entry) == 3) && (strcmp(kvCodecEntry_ValueString(entry), "cfg") == 0) && (memcmp(kvCodecEntry_Value(entry), "cfg", 3) == 0));
    kvCodecEntry_Destroy(entry);

    test("Raw store holds the subject form with the _root_ sentinel: ");
    s = kvStore_Get(&raw, kv, "_root_.config.app.database");
    testCond((s == NATS_OK) && (strcmp(kvEntry_ValueString(raw), "cfg") == 0));
    kvEntry_Destroy(raw);

    kvCodec_Destroy(c);
    kvKeyCodec_Destroy(keyCodec);
    KV_TEARDOWN;
}

void
test_KVNoOp(void)
{
    kvCodec      *c     = NULL;
    kvCodecEntry *entry = NULL;
    kvEntry      *raw   = NULL;

    KV_SETUP;

    test("NULL codecs mean passthrough: ");
    s = kvCodec_New(&c, kv, NULL, NULL);
    testCond(s == NATS_OK);

    test("PutString: ");
    s = kvCodec_PutString(NULL, c, "plain.key", "plain");
    testCond(s == NATS_OK);

    test("Codec view matches the raw store: ");
    s = kvCodec_Get(&entry, c, "plain.key");
    if (s == NATS_OK)
        s = kvStore_Get(&raw, kv, "plain.key");
    testCond((s == NATS_OK) && (strcmp(kvCodecEntry_Key(entry), kvEntry_Key(raw)) == 0) && (strcmp(kvCodecEntry_ValueString(entry), kvEntry_ValueString(raw)) == 0));
    kvCodecEntry_Destroy(entry);
    kvEntry_Destroy(raw);
    entry = NULL;

    test("Empty value round-trips as non-NULL, zero-length: ");
    s = kvCodec_PutString(NULL, c, "empty.key", "");
    if (s == NATS_OK)
        s = kvCodec_Get(&entry, c, "empty.key");
    testCond((s == NATS_OK) && (kvCodecEntry_Value(entry) != NULL) && (kvCodecEntry_ValueLen(entry) == 0) && (strcmp(kvCodecEntry_ValueString(entry), "") == 0));
    kvCodecEntry_Destroy(entry);

    kvCodec_Destroy(c);
    KV_TEARDOWN;
}

void
test_KVChain(void)
{
    kvKeyCodec     *path     = NULL;
    kvKeyCodec     *keyB64   = NULL;
    kvKeyCodec     *keyChain = NULL;
    kvKeyCodec     *keyMembers[2];
    kvValueCodec   *valB64   = NULL;
    kvValueCodec   *valChain = NULL;
    kvValueCodec   *valMembers[1];
    kvCodec        *c     = NULL;
    kvCodecEntry   *entry = NULL;
    kvCodecWatcher *w     = NULL;
    const char     *key   = "/config/app/setting";

    KV_SETUP;

    test("Create chains (key: path+base64, value: base64): ");
    s = kvKeyCodec_Path(&path);
    if (s == NATS_OK)
        s = kvKeyCodec_Base64(&keyB64);
    keyMembers[0] = path;
    keyMembers[1] = keyB64;
    if (s == NATS_OK)
        s = kvKeyCodec_Chain(&keyChain, keyMembers, 2);
    if (s == NATS_OK)
        s = kvValueCodec_Base64(&valB64);
    valMembers[0] = valB64;
    if (s == NATS_OK)
        s = kvValueCodec_Chain(&valChain, valMembers, 1);
    if (s == NATS_OK)
        s = kvCodec_New(&c, kv, keyChain, valChain);
    testCond(s == NATS_OK);

    test("Watch a path wildcard through the chain: ");
    s = kvCodec_Watch(&w, c, "/config/>", NULL);
    testCond(s == NATS_OK);

    test("Initial-values marker: ");
    s = kvCodecWatcher_Next(&entry, w, 1000);
    testCond((s == NATS_OK) && (entry == NULL));

    test("PutString: ");
    s = kvCodec_PutString(NULL, c, key, "42");
    testCond(s == NATS_OK);

    test("Watcher delivers the decoded path key: ");
    s = kvCodecWatcher_Next(&entry, w, 1000);
    testCond((s == NATS_OK) && (entry != NULL) && (strcmp(kvCodecEntry_Key(entry), key) == 0) && (strcmp(kvCodecEntry_ValueString(entry), "42") == 0));
    kvCodecEntry_Destroy(entry);
    entry = NULL;

    test("Get round-trips: ");
    s = kvCodec_Get(&entry, c, key);
    testCond((s == NATS_OK) && (strcmp(kvCodecEntry_Key(entry), key) == 0) && (strcmp(kvCodecEntry_ValueString(entry), "42") == 0));
    kvCodecEntry_Destroy(entry);

    kvCodecWatcher_Destroy(w);
    kvCodec_Destroy(c);
    kvValueCodec_Destroy(valChain);
    kvValueCodec_Destroy(valB64);
    kvKeyCodec_Destroy(keyChain);
    kvKeyCodec_Destroy(keyB64);
    kvKeyCodec_Destroy(path);
    KV_TEARDOWN;
}

void
test_KVGetNotFound(void)
{
    kvKeyCodec   *keyCodec = NULL;
    kvCodec      *c        = NULL;
    kvCodecEntry *entry    = NULL;

    KV_SETUP;

    test("Create codec wrapper: ");
    s = kvKeyCodec_Base64(&keyCodec);
    if (s == NATS_OK)
        s = kvCodec_New(&c, kv, keyCodec, NULL);
    testCond(s == NATS_OK);

    test("Underlying NATS_NOT_FOUND passes through: ");
    s = kvCodec_Get(&entry, c, "no.such.key");
    testCond(s == NATS_NOT_FOUND);

    kvCodec_Destroy(c);
    kvKeyCodec_Destroy(keyCodec);
    KV_TEARDOWN;
}

void
test_KVKeys(void)
{
    kvKeyCodec     *keyCodec = NULL;
    kvCodec        *c        = NULL;
    kvCodecKeysList keys     = { NULL, 0 };
    const char     *filter   = "user.*";
    bool            sawA = false, sawB = false, sawOther = false;
    int             i;

    KV_SETUP;

    test("Create codec wrapper: ");
    s = kvKeyCodec_Base64(&keyCodec);
    if (s == NATS_OK)
        s = kvCodec_New(&c, kv, keyCodec, NULL);
    testCond(s == NATS_OK);

    test("Populate: ");
    s = kvCodec_PutString(NULL, c, "user.a", "1");
    if (s == NATS_OK)
        s = kvCodec_PutString(NULL, c, "user.b", "2");
    if (s == NATS_OK)
        s = kvCodec_PutString(NULL, c, "other.c", "3");
    testCond(s == NATS_OK);

    test("Keys are decoded: ");
    s = kvCodec_Keys(&keys, c, NULL);
    for (i = 0; (s == NATS_OK) && (i < keys.Count); i++)
    {
        if (strcmp(keys.Keys[i], "user.a") == 0)
            sawA = true;
        else if (strcmp(keys.Keys[i], "user.b") == 0)
            sawB = true;
        else if (strcmp(keys.Keys[i], "other.c") == 0)
            sawOther = true;
    }
    testCond((s == NATS_OK) && (keys.Count == 3) && sawA && sawB && sawOther);
    kvCodecKeysList_Destroy(&keys);

    test("KeysWithFilters honors encoded wildcards: ");
    sawA = sawB = sawOther = false;
    s                      = kvCodec_KeysWithFilters(&keys, c, &filter, 1, NULL);
    for (i = 0; (s == NATS_OK) && (i < keys.Count); i++)
    {
        if (strcmp(keys.Keys[i], "user.a") == 0)
            sawA = true;
        else if (strcmp(keys.Keys[i], "user.b") == 0)
            sawB = true;
        else
            sawOther = true;
    }
    testCond((s == NATS_OK) && (keys.Count == 2) && sawA && sawB && !sawOther);
    kvCodecKeysList_Destroy(&keys);

    kvCodec_Destroy(c);
    kvKeyCodec_Destroy(keyCodec);
    KV_TEARDOWN;
}

void
test_KVWatch(void)
{
    kvKeyCodec     *keyCodec   = NULL;
    kvValueCodec   *valueCodec = NULL;
    kvCodec        *c          = NULL;
    kvCodecWatcher *w          = NULL;
    kvCodecEntry   *entry      = NULL;

    KV_SETUP;

    test("Create codec wrapper: ");
    s = kvKeyCodec_Base64(&keyCodec);
    if (s == NATS_OK)
        s = kvValueCodec_Base64(&valueCodec);
    if (s == NATS_OK)
        s = kvCodec_New(&c, kv, keyCodec, valueCodec);
    testCond(s == NATS_OK);

    test("Pre-populate one order: ");
    s = kvCodec_PutString(NULL, c, "orders.1", "pending");
    testCond(s == NATS_OK);

    test("Watch \"orders.>\": ");
    s = kvCodec_Watch(&w, c, "orders.>", NULL);
    testCond(s == NATS_OK);

    test("Initial value is delivered decoded: ");
    s = kvCodecWatcher_Next(&entry, w, 1000);
    testCond((s == NATS_OK) && (entry != NULL) && (strcmp(kvCodecEntry_Key(entry), "orders.1") == 0) && (strcmp(kvCodecEntry_ValueString(entry), "pending") == 0));
    kvCodecEntry_Destroy(entry);
    entry = NULL;

    test("Initial-values marker follows: ");
    s = kvCodecWatcher_Next(&entry, w, 1000);
    testCond((s == NATS_OK) && (entry == NULL));

    test("Live update arrives decoded: ");
    s = kvCodec_PutString(NULL, c, "orders.2", "shipped");
    if (s == NATS_OK)
        s = kvCodecWatcher_Next(&entry, w, 1000);
    testCond((s == NATS_OK) && (entry != NULL) && (strcmp(kvCodecEntry_Key(entry), "orders.2") == 0) && (strcmp(kvCodecEntry_ValueString(entry), "shipped") == 0));
    kvCodecEntry_Destroy(entry);
    entry = NULL;

    test("Non-matching key is not delivered, entry NULLed on timeout: ");
    entry = (kvCodecEntry *)0x1; // Next must reset it even on timeout
    s     = kvCodec_PutString(NULL, c, "users.1", "bob");
    if (s == NATS_OK)
        s = kvCodecWatcher_Next(&entry, w, 250);
    testCond((s == NATS_TIMEOUT) && (entry == NULL));

    test("Stop: ");
    s = kvCodecWatcher_Stop(w);
    testCond(s == NATS_OK);

    kvCodecWatcher_Destroy(w);
    kvCodec_Destroy(c);
    kvKeyCodec_Destroy(keyCodec);
    kvValueCodec_Destroy(valueCodec);
    KV_TEARDOWN;
}

void
test_KVWatchNonFilterable(void)
{
    kvKeyCodec     *keyCodec = NULL;
    kvCodec        *c        = NULL;
    kvCodecWatcher *w        = NULL;
    kvCodecEntry   *entry    = NULL;

    KV_SETUP;

    test("Create non-filterable custom codec (uppercase keys): ");
    s = kvKeyCodec_New(&keyCodec, _upperEncode, _lowerDecode, NULL, NULL, NULL);
    if (s == NATS_OK)
        s = kvCodec_New(&c, kv, keyCodec, NULL);
    testCond(s == NATS_OK);

    test("Wildcard watch is rejected: ");
    s = kvCodec_Watch(&w, c, "orders.*", NULL);
    testCond((s == NATS_INVALID_SUBJECT) && (w == NULL));

    test("Exact-key watch works: ");
    s = kvCodec_Watch(&w, c, "orders.neworder", NULL);
    testCond(s == NATS_OK);

    test("Entry key decodes back to lowercase: ");
    s = kvCodecWatcher_Next(&entry, w, 1000); // initial-values marker
    if ((s == NATS_OK) && (entry == NULL))
        s = kvCodec_PutString(NULL, c, "orders.neworder", "1");
    if (s == NATS_OK)
        s = kvCodecWatcher_Next(&entry, w, 1000);
    testCond((s == NATS_OK) && (entry != NULL) && (strcmp(kvCodecEntry_Key(entry), "orders.neworder") == 0));
    kvCodecEntry_Destroy(entry);

    kvCodecWatcher_Destroy(w);
    kvCodec_Destroy(c);
    kvKeyCodec_Destroy(keyCodec);
    KV_TEARDOWN;
}

void
test_KVTombstones(void)
{
    kvKeyCodec      *keyCodec   = NULL;
    kvValueCodec    *valueCodec = NULL;
    kvCodec         *c          = NULL;
    kvCodecEntryList history    = { NULL, 0 };
    kvCodecEntry    *marker     = NULL;

    KV_SETUP;

    test("Create codec wrapper: ");
    s = kvKeyCodec_Base64(&keyCodec);
    if (s == NATS_OK)
        s = kvValueCodec_Base64(&valueCodec);
    if (s == NATS_OK)
        s = kvCodec_New(&c, kv, keyCodec, valueCodec);
    testCond(s == NATS_OK);

    test("Put then delete: ");
    s = kvCodec_PutString(NULL, c, "doomed.key", "bye");
    if (s == NATS_OK)
        s = kvCodec_Delete(c, "doomed.key");
    testCond(s == NATS_OK);

    test("History carries the delete marker with a NULL value: ");
    s = kvCodec_History(&history, c, "doomed.key", NULL);
    if ((s == NATS_OK) && (history.Count == 2))
        marker = history.Entries[1];
    testCond((s == NATS_OK) && (history.Count == 2) && (marker != NULL) && (kvCodecEntry_Operation(marker) == kvOp_Delete) && (kvCodecEntry_Value(marker) == NULL) && (kvCodecEntry_ValueLen(marker) == 0) && (kvCodecEntry_ValueString(marker) == NULL) && (strcmp(kvCodecEntry_Key(marker), "doomed.key") == 0));
    kvCodecEntryList_Destroy(&history);

    kvCodec_Destroy(c);
    kvKeyCodec_Destroy(keyCodec);
    kvValueCodec_Destroy(valueCodec);
    KV_TEARDOWN;
}

void
test_KVPassThrough(void)
{
    kvKeyCodec    *keyCodec = NULL;
    kvCodec       *c        = NULL;
    kvStatus      *status   = NULL;
    kvPurgeOptions po;

    KV_SETUP;

    test("Create codec wrapper: ");
    s = kvKeyCodec_Base64(&keyCodec);
    if (s == NATS_OK)
        s = kvCodec_New(&c, kv, keyCodec, NULL);
    testCond(s == NATS_OK);

    test("Bucket: ");
    testCond((kvCodec_Bucket(c) != NULL) && (strcmp(kvCodec_Bucket(c), "TEST") == 0));

    test("Status: ");
    s = kvCodec_Status(&status, c);
    testCond((s == NATS_OK) && (strcmp(kvStatus_Bucket(status), "TEST") == 0));
    kvStatus_Destroy(status);

    test("PurgeDeletes: ");
    s = kvCodec_PutString(NULL, c, "gone.key", "x");
    if (s == NATS_OK)
        s = kvCodec_Delete(c, "gone.key");
    kvPurgeOptions_Init(&po);
    po.DeleteMarkersOlderThan = -1; // remove all markers regardless of age
    if (s == NATS_OK)
        s = kvCodec_PurgeDeletes(c, &po);
    testCond(s == NATS_OK);

    kvCodec_Destroy(c);
    kvKeyCodec_Destroy(keyCodec);
    KV_TEARDOWN;
}

//=============================================================================
// main
//=============================================================================

int
main(int argc, char **argv)
{
    return testMain(argc, argv);
}
