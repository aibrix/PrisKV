#include <assert.h>
#include <stdio.h>

#include "priskv-ucx.h"

static int test_priskv_ucx_tls_has_cuda_disabled(const char *tls, int expect_disabled)
{
    int d = priskv_ucx_tls_has_cuda_disabled(tls);
    printf("TLS='%s' disabled=%d expect=%d\n", tls ? tls : "(null)", d, expect_disabled);
    fflush(stdout);
    return d == expect_disabled ? 0 : 1;
}

int main(void)
{
    int fails = 0;
    fails += test_priskv_ucx_tls_has_cuda_disabled("^cuda", 1);
    fails += test_priskv_ucx_tls_has_cuda_disabled("^cuda_copy", 1);
    fails += test_priskv_ucx_tls_has_cuda_disabled("^cuda_ipc", 0);
    fails += test_priskv_ucx_tls_has_cuda_disabled("all", 0);
    fails += test_priskv_ucx_tls_has_cuda_disabled("rc,sm,self", 1);
    fails += test_priskv_ucx_tls_has_cuda_disabled("rc,cuda_ipc", 0);
    fails += test_priskv_ucx_tls_has_cuda_disabled("cuda", 0);
    fails += test_priskv_ucx_tls_has_cuda_disabled("cuda_copy", 0);
    fails += test_priskv_ucx_tls_has_cuda_disabled(" cuda_copy , rc ", 0);
    fails += test_priskv_ucx_tls_has_cuda_disabled(" ^ cuda_copy ", 1);
    fails += test_priskv_ucx_tls_has_cuda_disabled(" ^cuda_copy", 1);
    fails += test_priskv_ucx_tls_has_cuda_disabled("", 1);
    if (fails == 0) {
        printf("ok\n");
        return 0;
    }
    printf("failed=%d\n", fails);
    return 1;
}
