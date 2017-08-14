/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

/* Define UNIT_TESTING so that the extension can skip declaring PG_MODULE_MAGIC */
#define UNIT_TESTING

/* include unit under test */
#include "../src/pxfuriparser.c"

/* include mock files */
#include "mock/pxfutils_mock.c"


static void expect_normalize_key_name(const char *key);
static void test_parseGPHDUri_helper(const char* uri, const char* message);
static void test_parseFragment_helper(const char* fragment, const char* message);

static char uri_no_segwork[] = "pxf://1.2.3.4:5678/some/path/and/table.tbl?FRAGMENTER=SomeFragmenter&ACCESSOR=SomeAccessor&RESOLVER=SomeResolver&ANALYZER=SomeAnalyzer";
static char uri_with_segwork_1[] = "pxf://1.2.3.4:5678/some/path/and/table.tbl?FRAGMENTER=SomeFragmenter&ACCESSOR=SomeAccessor&RESOLVER=SomeResolver&ANALYZER=SomeAnalyzer&segwork=42@127.0.0.1@51200@tmp/test@0@ZnJhZ21lbnQx@@@";
static char uri_with_segwork_2[] = "pxf://1.2.3.4:5678/some/path/and/table.tbl?FRAGMENTER=SomeFragmenter&ACCESSOR=SomeAccessor&RESOLVER=SomeResolver&ANALYZER=SomeAnalyzer&segwork=42@127.0.0.1@51200@tmp/test@0@ZnJhZ21lbnQx@@@41@127.0.0.1@51200@tmp/foo@0@ZnJhZ21lbnQx@@@";

/*
 * Test parsing of valid uri as given in LOCATION in a PXF external table.
 */
void
test_parseGPHDUri_ValidURI(void **state)
{
    List* options = NIL;
    ListCell* cell = NULL;
    OptionData* option = NULL;

    expect_normalize_key_name("FRAGMENTER");
    expect_normalize_key_name("ACCESSOR");
    expect_normalize_key_name("RESOLVER");
    expect_normalize_key_name("ANALYZER");

    GPHDUri* parsed = parseGPHDUri(uri_no_segwork);

    assert_true(parsed != NULL);
    assert_string_equal(parsed->uri, uri_no_segwork);

    assert_string_equal(parsed->protocol, "pxf");
    assert_string_equal(parsed->host, "localhost");
    assert_string_equal(parsed->port, "51200");
    assert_string_equal(parsed->data, "some/path/and/table.tbl");

    options = parsed->options;
    assert_int_equal(list_length(options), 4);

    cell = list_nth_cell(options, 0);
    option = lfirst(cell);
    assert_string_equal(option->key, "FRAGMENTER");
    assert_string_equal(option->value, "SomeFragmenter");

    cell = list_nth_cell(options, 1);
    option = lfirst(cell);
    assert_string_equal(option->key, "ACCESSOR");
    assert_string_equal(option->value, "SomeAccessor");

    cell = list_nth_cell(options, 2);
    option = lfirst(cell);
    assert_string_equal(option->key, "RESOLVER");
    assert_string_equal(option->value, "SomeResolver");

    cell = list_nth_cell(options, 3);
    option = lfirst(cell);
    assert_string_equal(option->key, "ANALYZER");
    assert_string_equal(option->value, "SomeAnalyzer");

    assert_true(parsed->fragments == NULL);

    assert_true(parsed->profile == NULL);

    GPHDUri_free_fragments(parsed);
    freeGPHDUri(parsed);
}

/*
 * Negative test: parsing of uri without protocol delimiter "://"
 */
void
test_parseGPHDUri_NegativeTestNoProtocol(void **state)
{
    char* uri = "pxf:/1.2.3.4:5678/some/path/and/table.tbl?FRAGMENTER=HdfsDataFragmenter";
    test_parseGPHDUri_helper(uri, "");
}

/*
 * Negative test: parsing of uri without options part
 */
void
test_parseGPHDUri_NegativeTestNoOptions(void **state)
{
    char *uri = "pxf://1.2.3.4:5678/some/path/and/table.tbl";
    test_parseGPHDUri_helper(uri, ": missing options section");
}

/*
 * Negative test: parsing of a uri with a missing equal
 */
void
test_parseGPHDUri_NegativeTestMissingEqual(void **state)
{
    char* uri = "pxf://1.2.3.4:5678/some/path/and/table.tbl?FRAGMENTER";
    test_parseGPHDUri_helper(uri, ": option 'FRAGMENTER' missing '='");
}

/*
 * Negative test: parsing of a uri with duplicate equals
 */
void
test_parseGPHDUri_NegativeTestDuplicateEquals(void **state)
{
    char* uri = "pxf://1.2.3.4:5678/some/path/and/table.tbl?FRAGMENTER=HdfsDataFragmenter=DuplicateFragmenter";
    test_parseGPHDUri_helper(uri, ": option 'FRAGMENTER=HdfsDataFragmenter=DuplicateFragmenter' contains duplicate '='");
}

/*
 * Negative test: parsing of a uri with a missing key
 */
void
test_parseGPHDUri_NegativeTestMissingKey(void **state)
{
    char* uri = "pxf://1.2.3.4:5678/some/path/and/table.tbl?=HdfsDataFragmenter";
    test_parseGPHDUri_helper(uri, ": option '=HdfsDataFragmenter' missing key before '='");
}

/*
 * Negative test: parsing of a uri with a missing value
 */
void
test_parseGPHDUri_NegativeTestMissingValue(void **state)
{
    char* uri = "pxf://1.2.3.4:5678/some/path/and/table.tbl?FRAGMENTER=";
    test_parseGPHDUri_helper(uri, ": option 'FRAGMENTER=' missing value after '='");
}

/*
 * Test GPHDUri_parse_fragment when fragment string is valid and all parameters are passed
 */
void
test_GPHDUri_parse_fragment_ValidFragment(void **state)
{
    char* fragment = "HOST@REST_PORT@TABLE_NAME@INDEX@FRAGMENT_METADATA@USER_DATA@PROFILE@";

    List *fragments = NIL;

    fragments = GPHDUri_parse_fragment(fragment, fragments);

    ListCell *fragment_cell = list_head(fragments);
    FragmentData *fragment_data = (FragmentData*) lfirst(fragment_cell);

    assert_string_equal(fragment_data->authority, "HOST:REST_PORT");
    assert_string_equal(fragment_data->fragment_md, "FRAGMENT_METADATA");
    assert_string_equal(fragment_data->index, "INDEX");
    assert_string_equal(fragment_data->profile, "PROFILE");
    assert_string_equal(fragment_data->source_name, "TABLE_NAME");
    assert_string_equal(fragment_data->user_data, "USER_DATA");
    assert_true(fragment_cell->next == NULL);

    GPHDUri_free_fragment(fragment_data);
    list_free(fragments);
}

/*
 * Test GPHDUri_parse_fragment when fragment string doesn't have profile
 */
void
test_GPHDUri_parse_fragment_EmptyProfile(void **state)
{
    char* fragment = "HOST@REST_PORT@TABLE_NAME@INDEX@FRAGMENT_METADATA@USER_DATA@@";

    List *fragments = NIL;

    fragments = GPHDUri_parse_fragment(fragment, fragments);

    ListCell *fragment_cell = list_head(fragments);
    FragmentData *fragment_data = (FragmentData*) lfirst(fragment_cell);

    assert_string_equal(fragment_data->authority, "HOST:REST_PORT");
    assert_string_equal(fragment_data->fragment_md, "FRAGMENT_METADATA");
    assert_string_equal(fragment_data->index, "INDEX");
    assert_true(!fragment_data->profile);
    assert_string_equal(fragment_data->source_name, "TABLE_NAME");
    assert_string_equal(fragment_data->user_data, "USER_DATA");

    GPHDUri_free_fragment(fragment_data);
    list_free(fragments);

}

/*
 * Test GPHDUri_parse_fragment when fragment string is empty
 */
void
test_GPHDUri_parse_fragment_EmptyString(void **state)
{
    char* fragment = "";
    test_parseFragment_helper(fragment, "internal error in pxfuriparser.c:GPHDUri_parse_fragment. Fragment string is invalid.");
}

/*
 * Test GPHDUri_parse_fragment when fragment string is null
 */
void
test_GPHDUri_parse_fragment_NullFragment(void **state)
{
    char *fragment = NULL;
    test_parseFragment_helper(fragment, "internal error in pxfuriparser.c:GPHDUri_parse_fragment. Fragment string is null.");
}

/*
 * Test GPHDUri_parse_fragment when fragment string has less tokens then expected
 */
void
test_GPHDUri_parse_fragment_MissingIpHost(void **state)
{
    char* fragment = "@";
    test_parseFragment_helper(fragment, "internal error in pxfuriparser.c:GPHDUri_parse_fragment. Fragment string is invalid.");
}

/*
 * Test GPHDUri_parse_fragment when fragment string has less tokens then expected
 */
void
test_GPHDUri_parse_fragment_MissingPort(void **state)
{
    char* fragment = "@HOST@";
    test_parseFragment_helper(fragment, "internal error in pxfuriparser.c:GPHDUri_parse_fragment. Fragment string is invalid.");
}

/*
 * Test GPHDUri_parse_fragment when fragment string has less tokens then expected
 */
void
test_GPHDUri_parse_fragment_MissingSourceName(void **state)
{
    char* fragment = "@HOST@PORT@";
    test_parseFragment_helper(fragment, "internal error in pxfuriparser.c:GPHDUri_parse_fragment. Fragment string is invalid.");
}

/*
 * Test GPHDUri_parse_fragment when fragment string has less tokens then expected
 */
void
test_GPHDUri_parse_fragment_MissingIndex(void **state)
{
    char* fragment = "@HOST@PORT@SOURCE_NAME@";
    test_parseFragment_helper(fragment, "internal error in pxfuriparser.c:GPHDUri_parse_fragment. Fragment string is invalid.");
}

/*
 * Test GPHDUri_parse_fragment when fragment string has less tokens then expected
 */
void
test_GPHDUri_parse_fragment_MissingFragmentMetadata(void **state)
{
    char* fragment = "@HOST@PORT@SOURCE_NAME@42@";
    test_parseFragment_helper(fragment, "internal error in pxfuriparser.c:GPHDUri_parse_fragment. Fragment string is invalid.");
}

/*
 * Test GPHDUri_parse_fragment when fragment string has less tokens then expected
 */
void
test_GPHDUri_parse_fragment_MissingUserData(void **state)
{
    char* fragment = "HOST@REST_PORT@TABLE_NAME@INDEX@FRAGMENT_METADATA@";
    test_parseFragment_helper(fragment, "internal error in pxfuriparser.c:GPHDUri_parse_fragment. Fragment string is invalid.");
}

/*
 * Test GPHDUri_parse_fragment when fragment string has less tokens then expected
 */
void
test_GPHDUri_parse_fragment_MissingProfile(void **state)
{
    char* fragment = "HOST@REST_PORT@TABLE_NAME@INDEX@FRAGMENT_METADATA@USER_METADATA@";
    test_parseFragment_helper(fragment, "internal error in pxfuriparser.c:GPHDUri_parse_fragment. Fragment string is invalid.");
}

void
test_GPHDUri_parse_segwork_NoSegwork(void **state)
{
    GPHDUri	*uri = (GPHDUri *)palloc0(sizeof(GPHDUri));
    GPHDUri_parse_segwork(uri, uri_no_segwork);
    assert_true(uri->fragments == NULL);
    pfree(uri);
}

void
test_GPHDUri_parse_segwork_TwoFragments(void **state)
{
    GPHDUri	*uri = (GPHDUri *)palloc0(sizeof(GPHDUri));

    GPHDUri_parse_segwork(uri, uri_with_segwork_2);

    assert_true(uri->fragments != NULL);
    assert_int_equal(uri->fragments->length, 2);
    assert_string_equal(((FragmentData*)linitial(uri->fragments))->source_name, "tmp/test");
    assert_string_equal(((FragmentData*)lsecond(uri->fragments))->source_name, "tmp/foo");

    list_free_deep(uri->fragments);
    pfree(uri);
}

void
test_GPHDUri_parse_segwork(void **state)
{
    GPHDUri	*uri = (GPHDUri *)palloc0(sizeof(GPHDUri));
    pfree(uri);
}

void
test_GPHDUri_opt_exists(void **state)
{
    char* uri_str = "xyz?FRAGMENTER=HdfsDataFragmenter&RESOLVER=SomeResolver";
    char* cursor = uri_str + 3; // location of '?'
    GPHDUri	*uri = (GPHDUri *)palloc0(sizeof(GPHDUri));
    expect_normalize_key_name("FRAGMENTER");
    expect_normalize_key_name("RESOLVER");
    GPHDUri_parse_options(uri, &cursor);

    int exists = GPHDUri_opt_exists(uri, "FRAGMENTER");
    assert_int_equal(exists, 0);
    exists = GPHDUri_opt_exists(uri, "RESOLVER");
    assert_int_equal(exists, 0);
    exists = GPHDUri_opt_exists(uri, "ACCESSOR");
    assert_int_equal(exists, -1);

    pfree(uri);
}

/*
 * Helper function for parse fragment test cases
 */
static void
test_parseFragment_helper(const char* fragment, const char* message)
{
    List *fragments = NIL;

    MemoryContext old_context = CurrentMemoryContext;
    PG_TRY();
    {
        /* This will throw a ereport(ERROR).*/
        fragments = GPHDUri_parse_fragment(fragment, fragments);
        assert_false("Expected Exception");
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(old_context);
        ErrorData *edata = CopyErrorData();
        FlushErrorState();

        StringInfo err_msg = makeStringInfo();
        appendStringInfo(err_msg, message);

        /* Validate the type of expected error */
        assert_true(edata->sqlerrcode == ERRCODE_INTERNAL_ERROR);
        assert_true(edata->elevel == ERROR);
        assert_string_equal(edata->message, err_msg->data);

        list_free(fragments);
        pfree(err_msg->data);
        pfree(err_msg);
        elog_dismiss(INFO);
    }
    PG_END_TRY();
}

/*
 * Helper function for parse uri test cases
 */
static void
test_parseGPHDUri_helper(const char* uri, const char* message)
{
    /* Setting the test -- code omitted -- */
    MemoryContext old_context = CurrentMemoryContext;
    PG_TRY();
    {
        /* This will throw a ereport(ERROR).*/
        GPHDUri* parsed = parseGPHDUri(uri);
        assert_false("Expected Exception");
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(old_context);
        ErrorData *edata = CopyErrorData();
        FlushErrorState();

        /*Validate the type of expected error */
        assert_true(edata->sqlerrcode == ERRCODE_SYNTAX_ERROR);
        assert_true(edata->elevel == ERROR);
        StringInfoData expected_message;
        initStringInfo(&expected_message);
        appendStringInfo(&expected_message, "Invalid URI %s%s", uri, message);

        assert_string_equal(edata->message, expected_message.data);
        pfree(expected_message.data);
        elog_dismiss(INFO);
    }
    PG_END_TRY();
}

static void
expect_normalize_key_name(const char *key)
{
    StringInfoData keyname;
    initStringInfo(&keyname);
    appendStringInfo(&keyname, "X-GP-%s", key);
    expect_string(normalize_key_name, key, key);
    will_return(normalize_key_name, keyname.data);
}

int
main(int argc, char* argv[])
{
    cmockery_parse_arguments(argc, argv);

    const UnitTest tests[] = {
            unit_test(test_parseGPHDUri_ValidURI),
            unit_test(test_parseGPHDUri_NegativeTestNoProtocol),
            unit_test(test_parseGPHDUri_NegativeTestNoOptions),
            unit_test(test_parseGPHDUri_NegativeTestMissingEqual),
            unit_test(test_parseGPHDUri_NegativeTestDuplicateEquals),
            unit_test(test_parseGPHDUri_NegativeTestMissingKey),
            unit_test(test_parseGPHDUri_NegativeTestMissingValue),
            unit_test(test_GPHDUri_parse_fragment_EmptyProfile),
            unit_test(test_GPHDUri_parse_fragment_ValidFragment),
            unit_test(test_GPHDUri_parse_fragment_EmptyString),
            unit_test(test_GPHDUri_parse_fragment_NullFragment),
            unit_test(test_GPHDUri_parse_fragment_MissingIpHost),
            unit_test(test_GPHDUri_parse_fragment_MissingPort),
            unit_test(test_GPHDUri_parse_fragment_MissingSourceName),
            unit_test(test_GPHDUri_parse_fragment_MissingIndex),
            unit_test(test_GPHDUri_parse_fragment_MissingFragmentMetadata),
            unit_test(test_GPHDUri_parse_fragment_MissingUserData),
            unit_test(test_GPHDUri_parse_fragment_MissingProfile),
            unit_test(test_GPHDUri_parse_segwork_NoSegwork),
            unit_test(test_GPHDUri_parse_segwork_TwoFragments),
            unit_test(test_GPHDUri_opt_exists)
    };

    MemoryContextInit();

    return run_tests(tests);
}
