#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

/* Define UNIT_TESTING so that the extension can skip declaring PG_MODULE_MAGIC */
#define UNIT_TESTING

/* include unit under test */
#include "../src/pxffragment.c"

/* include mock files */
#include "mock/libchurl_mock.c"
#include "mock/pxfheaders_mock.c"
#include "mock/pxfuriparser_mock.c"
#include "mock/pxfutils_mock.c"

#define ARRSIZE(x) (sizeof(x) / sizeof((x)[0]))

/* helper functions */
static List* prepare_fragment_list(int fragtotal, int segindex, int segtotal, int xid);
static List* prepare_fragment_list_with_replicas(int fragtotal, int segindex, int segtotal, int xid, int num_replicas);
static void print_list(List* list);
static void test_list(int segindex, int segtotal, int xid, int fragtotal, int expected[], int expected_total);

void
test_filter_fragments_for_segment(void **state)
{

    /* --- 1 segment, all fragements should be processed by it */
    int expected_1_1_0[1] = {0}; // 1 fragment
    test_list(0, 1, 1, 1, expected_1_1_0, ARRSIZE(expected_1_1_0)); // xid=1
    test_list(0, 1, 2, 1, expected_1_1_0, ARRSIZE(expected_1_1_0)); // xid=2

    int expected_1_2_0[2] = {0,1};  // 2 fragments
    test_list(0, 1, 1, 2, expected_1_2_0, ARRSIZE(expected_1_2_0)); // xid=1
    test_list(0, 1, 2, 2, expected_1_2_0, ARRSIZE(expected_1_2_0)); // xid=2

    int expected_1_3_0[3] = {0,1,2};  // 3 fragments
    test_list(0, 1, 1, 3, expected_1_3_0, ARRSIZE(expected_1_3_0)); // xid=1
    test_list(0, 1, 2, 3, expected_1_3_0, ARRSIZE(expected_1_3_0)); // xid=2

    /* --- 2 segments, each processes every other fragment, based on xid */
    // 1 fragment, xid=1
    test_list(0, 2, 1, 1, NULL, 0); // seg=0
    int expected_1_2_1_1[1] = {0}; // seg=1
    test_list(1, 2, 1, 1, expected_1_2_1_1, ARRSIZE(expected_1_2_1_1));

    // 1 fragment, xid=2
    int expected_0_2_2_1[1] = {0}; // seg=0
    test_list(0, 2, 2, 1, expected_0_2_2_1, ARRSIZE(expected_0_2_2_1));
    test_list(1, 2, 2, 1, NULL, 0); // seg=1

    // 1 fragment, xid=3
    test_list(0, 2, 3, 1, NULL, 0); // seg=0
    int expected_1_2_3_1[1] = {0}; // seg=1
    test_list(1, 2, 3, 1, expected_1_2_3_1, ARRSIZE(expected_1_2_3_1));

    // 2 fragments, xid=1
    int expected_0_2_1_2[1] = {1}; // seg=0
    test_list(0, 2, 1, 2, expected_0_2_1_2, ARRSIZE(expected_0_2_1_2));
    int expected_1_2_1_2[1] = {0}; // seg=1
    test_list(1, 2, 1, 2, expected_1_2_1_2, ARRSIZE(expected_1_2_1_2));

    // 2 fragments, xid=2
    int expected_0_2_2_2[1] = {0}; // seg=0
    test_list(0, 2, 2, 2, expected_0_2_2_2, ARRSIZE(expected_0_2_2_2));
    int expected_1_2_2_2[1] = {1}; // seg=1
    test_list(1, 2, 2, 2, expected_1_2_2_2, ARRSIZE(expected_1_2_2_2));

    // 2 fragments, xid=3
    int expected_0_2_3_2[1] = {1}; // seg=0
    test_list(0, 2, 3, 2, expected_0_2_3_2, ARRSIZE(expected_0_2_3_2));
    int expected_1_2_3_2[1] = {0}; // seg=1
    test_list(1, 2, 3, 2, expected_1_2_3_2, ARRSIZE(expected_1_2_3_2));

    // 3 fragments, xid=1
    int expected_0_2_1_3[1] = {1}; // seg=0
    test_list(0, 2, 1, 3, expected_0_2_1_3, ARRSIZE(expected_0_2_1_3));
    int expected_1_2_1_3[2] = {0,2}; // seg=1
    test_list(1, 2, 1, 3, expected_1_2_1_3, ARRSIZE(expected_1_2_1_3));

    // 3 fragments, xid=2
    int expected_0_2_2_3[2] = {0,2}; // seg=0
    test_list(0, 2, 2, 3, expected_0_2_2_3, ARRSIZE(expected_0_2_2_3));
    int expected_1_2_2_3[1] = {1}; // seg=1
    test_list(1, 2, 2, 3, expected_1_2_2_3, ARRSIZE(expected_1_2_2_3));

    // 3 fragments, xid=3
    int expected_0_2_3_3[1] = {1}; // seg=0
    test_list(0, 2, 3, 3, expected_0_2_3_3, ARRSIZE(expected_0_2_3_3));
    int expected_1_2_3_3[2] = {0,2}; // seg=1
    test_list(1, 2, 3, 3, expected_1_2_3_3, ARRSIZE(expected_1_2_3_3));

    /* special case -- no fragments */
    MemoryContext old_context = CurrentMemoryContext;
    PG_TRY();
    {
        test_list(0, 1, 1, 0, NULL, 0);
        assert_false("Expected Exception");
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(old_context);
        ErrorData *edata = CopyErrorData();
        assert_true(edata->elevel == ERROR);
        char* expected_message = pstrdup("internal error in pxffragment.c:filter_fragments_for_segment. Parameter list is null.");
        assert_string_equal(edata->message, expected_message);
        pfree(expected_message);
    }
    PG_END_TRY();

    /* special case -- invalid transaction id */
    old_context = CurrentMemoryContext;
    PG_TRY();
    {
        test_list(0, 1, 0, 1, NULL, 0);
        assert_false("Expected Exception");
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(old_context);
        ErrorData *edata = CopyErrorData();
        assert_true(edata->elevel == ERROR);
        char* expected_message = pstrdup("internal error in pxffragment.c:filter_fragments_for_segment. Cannot get distributed transaction identifier.");
        assert_string_equal(edata->message, expected_message);
        pfree(expected_message);
    }
    PG_END_TRY();
}

static void
test_list(int segindex, int segtotal, int xid, int fragtotal, int expected[], int expected_total)
{
    /* prepare the input list */
    List* list = prepare_fragment_list(fragtotal, segindex, segtotal, xid);

    /* filter the list */
    List* filtered = filter_fragments_for_segment(list);

    /* assert results */
    if (expected_total > 0)
    {
        assert_int_equal(filtered->length, expected_total);

        ListCell* cell;
        int i;
        foreach_with_count(cell, filtered, i)
        {
            assert_int_equal(((DataFragment *) lfirst(cell))->index, expected[i]);
        }
    }
    else
    {
        assert_true(filtered == NIL);
    }
}

static List*
prepare_fragment_list(int fragtotal, int segindex, int segtotal, int xid)
{
    GpIdentity.segindex = segindex;
    GpIdentity.numsegments = segtotal;

    if (fragtotal > 0)
        will_return(getDistributedTransactionId, xid);

    List* result = NIL;
    for (int i=0; i<fragtotal; i++) {
        DataFragment* fragment = palloc0(sizeof(DataFragment));
        fragment->index = i;
        result = lappend(result, fragment);
    }
    return result;
}

static List*
prepare_fragment_list_with_replicas(int fragtotal, int segindex, int segtotal, int xid, int num_replicas)
{
    GpIdentity.segindex = segindex;
    GpIdentity.numsegments = segtotal;

    will_return(getDistributedTransactionId, xid);

    List* result = NIL;

    for (int i=0; i<fragtotal; i++) {
    	DataFragment* fragment = (DataFragment*)palloc0(sizeof(DataFragment));
        fragment->index = i;
		for (int j=0; j<num_replicas; j++)
		{
			FragmentHost* fhost = (FragmentHost*)palloc(sizeof(FragmentHost));
			fhost->ip = pstrdup(PXF_HOST);
			fhost->rest_port = PXF_PORT;
			fragment->replicas = lappend(fragment->replicas, fhost);
		}
		result = lappend(result, fragment);
    }
    return result;
}

void
test_assign_pxf_location_to_fragments(void **state)
{
	List* list = prepare_fragment_list_with_replicas(2, 0, 1, 9, 3);
	assign_pxf_location_to_fragments(list);
	for(int i=0; i<2; i++)
	{
		List *replica_list = ((DataFragment *)linitial(list))->replicas;
//		assert_string_equal(, PXF_HOST);
//
//
//		assert_int_equal((DataFragment *)(linitial(list))->rest_port, PXF_PORT);
//		assert_int_equal(length((DataFragment *)(linitial(list)))->replicas), 3);
	}
}

//TODO remove helper function
static void
print_list(List* list)
{
    ListCell* cell;
    printf("list:\n");
    if (! list) {
        printf("NIL\n");
        return;
    }
    foreach(cell, list)
    {
        printf("%d,", ((DataFragment *) (cell->data.ptr_value))->index);
    }
    printf("\n");
}

int
main(int argc, char* argv[])
{
    cmockery_parse_arguments(argc, argv);

    const UnitTest tests[] = {
            unit_test(test_filter_fragments_for_segment)
    };

    MemoryContextInit();

    return run_tests(tests);
}
