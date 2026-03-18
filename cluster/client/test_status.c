// Simple consistency test: verify numeric and string mappings between single-node status and cluster status
// Run: make -C cluster && ./cluster/client/priskv-cluster-test-status

#include <stdio.h>
#include <string.h>

#include "priskv.h"
#include "client.h"

static int check_one(priskv_status s)
{
    priskvClusterStatus cs = priskvClusterStatusFromPriskvStatus(s);
    const char *ss = priskv_status_str(s);
    const char *cs_str = priskv_cluster_status_str(cs);

    int ok = 1;

    if (cs != (priskvClusterStatus)s) {
        fprintf(stderr, "[NUMERIC] mismatch: priskv=%d cluster=%d\n", (int)s, (int)cs);
        ok = 0;
    }

    if (strcmp(ss, cs_str) != 0) {
        fprintf(stderr, "[STRING] mismatch: priskv=\"%s\" cluster=\"%s\" (code=%d)\n", ss, cs_str,
                (int)s);
        ok = 0;
    }

    return ok;
}

int main(void)
{
    priskv_status cases[] = {
        PRISKV_STATUS_OK,
        PRISKV_STATUS_INVALID_COMMAND,
        PRISKV_STATUS_KEY_EMPTY,
        PRISKV_STATUS_KEY_TOO_BIG,
        PRISKV_STATUS_VALUE_EMPTY,
        PRISKV_STATUS_VALUE_TOO_BIG,
        PRISKV_STATUS_NO_SUCH_COMMAND,
        PRISKV_STATUS_NO_SUCH_KEY,
        PRISKV_STATUS_NO_SUCH_TOKEN,
        PRISKV_STATUS_INVALID_SGL,
        PRISKV_STATUS_INVALID_REGEX,
        PRISKV_STATUS_KEY_UPDATING,
        PRISKV_STATUS_CONNECT_ERROR,
        PRISKV_STATUS_SERVER_ERROR,
        PRISKV_STATUS_PERMISSION_DENIED,
        PRISKV_STATUS_NO_MEM,
        PRISKV_STATUS_DISCONNECTED,
        PRISKV_STATUS_TRANSPORT_ERROR,
        PRISKV_STATUS_BUSY,
        PRISKV_STATUS_PROTOCOL_ERROR,
    };

    int pass = 1;
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
        if (!check_one(cases[i])) {
            pass = 0;
        }
    }

    if (!pass) {
        fprintf(stderr, "Status consistency test: FAILED\n");
        return 1;
    }

    printf("Status consistency test: PASSED\n");
    return 0;
}
