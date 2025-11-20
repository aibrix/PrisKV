// Copyright (c) 2025 ByteDance Ltd. and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/*
 * Authors:
 *   Jinlong Xuan <15563983051@163.com>
 *   Xu Ji <sov.matrixac@gmail.com>
 *   Yu Wang <wangyu.steph@bytedance.com>
 *   Bo Liu <liubo.2024@bytedance.com>
 *   Zhenwei Pi <pizhenwei@bytedance.com>
 *   Rui Zhang <zhangrui.1203@bytedance.com>
 *   Changqi Lu <luchangqi.123@bytedance.com>
 *   Enhua Zhou <zhouenhua@bytedance.com>
 */

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "slab.h"

#define TEST_OBJECTS 4096

static int test_round(uint32_t size)
{
    uint32_t objects = TEST_OBJECTS;
    uint8_t *objs[TEST_OBJECTS] = {NULL}, *ptr;
    const char *name = "test-slab";
    void *slab;
    uint8_t *base;

    base = calloc(objects, size);
    assert(base);

    slab = priskv_slab_create(name, base, size, objects);
    assert(slab);
    assert(priskv_slab_base(slab) == base);
    assert(priskv_slab_size(slab) == size);
    assert(priskv_slab_objects(slab) == objects);
    assert(!strcmp(priskv_slab_name(slab), name));

    /* step 1, allocate all the objects */
    for (uint32_t i = 0; i < sizeof(objs) / sizeof(objs[0]); i++) {
        objs[i] = priskv_slab_alloc(slab);
        if (i < objects) {
            assert(objs[i] - base == (uint64_t)size * i);
        } else {
            assert(!objs[i]);
        }
    }

    assert(priskv_slab_inuse(slab) == sizeof(objs) / sizeof(objs[0]));

    /* step 2, check address of all the objects */
    for (uint32_t i = 0; i < objects; i++) {
        assert(priskv_slab_index(slab, objs[i]) == i);
        assert(priskv_slab_index(slab, objs[i] + 4) == -1);
    }

    /* step 3, free one and re-allocate one, it should be the same one */
    for (uint32_t i = 0; i < objects; i++) {
        ptr = objs[random() % objects];
        priskv_slab_free(slab, ptr);
        assert(priskv_slab_alloc(slab) == ptr);
    }

    assert(priskv_slab_inuse(slab) == sizeof(objs) / sizeof(objs[0]));

    /* step 4, free all the objects */
    for (uint32_t i = 0; i < objects; i++) {
        ptr = objs[i];
        priskv_slab_free(slab, ptr);
    }

    assert(priskv_slab_inuse(slab) == 0);
    memset(objs, 0x00, sizeof(objs));

    /* step 4, free all the objects */
    for (uint32_t i = 0; i < objects; i++) {
        objs[i] = priskv_slab_reserve(slab, i);
        assert(priskv_slab_index(slab, objs[i]) == i);
    }

    priskv_slab_destroy(slab);
    free(base);
    return 0;
}

int main()
{
    /* round 1: test a small slab */
    assert(!test_round(128));

    /* round 2: test a huge slab */
    assert(!test_round(1024 * 1024 * 2));
}
