/*
  $Id$
*/
#include <check.h>
#include "test.h"

extern struct test_suite test_suite_base;

struct test_suite *suites[] = {

    &test_suite_base,
    NULL,
};

Suite *make_suite(struct test_suite *tsuite)
{
    Suite *s = suite_create(tsuite->name);
    int i = 0;

    while (tsuite->cases[i].name) {
        TCase *tc = tcase_create(tsuite->cases[i].name);
        tcase_add_test(tc, tsuite->cases[i].test_fn);
        suite_add_tcase(s, tc);
        //printf("%s add %s\n", tsuite->name, tsuite->cases[i].name);
        i++;
    }
    return s;
}

int main(int argc, char *argv[])
{
    int i = 0, nerr = 0;
    
    while (suites[i]) {
        Suite *s = make_suite(suites[i]);
        SRunner *sr = srunner_create(s);
        printf("\n");
        srunner_run_all(sr, CK_NORMAL);
        nerr += srunner_ntests_failed(sr);
        srunner_free(sr);
        i++;
    }

    return (nerr == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}

