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
 *
 */

#include "pxfbridge.h"
#include "pxfuriparser.h"
#include "access/extprotocol.h"

/* define magic module unless run as a part of test cases */
#ifndef UNIT_TESTING
PG_MODULE_MAGIC;
#endif

PG_FUNCTION_INFO_V1(pxfprotocol_export);
PG_FUNCTION_INFO_V1(pxfprotocol_import);
PG_FUNCTION_INFO_V1(pxfprotocol_validate_urls);

/* public function declarations */
Datum pxfprotocol_export(PG_FUNCTION_ARGS);
Datum pxfprotocol_import(PG_FUNCTION_ARGS);
Datum pxfprotocol_validate_urls(PG_FUNCTION_ARGS);

/* helper function declarations */
static gphadoop_context* create_context(PG_FUNCTION_ARGS, bool is_import);
static void cleanup_context(gphadoop_context *context);
static void check_caller(PG_FUNCTION_ARGS, const char* func_name);

/*
 * Validates external table URL
 */
Datum
pxfprotocol_validate_urls(PG_FUNCTION_ARGS)
{
    GPHDUri	*uri;
    char *option;
    char *fragmenter = NULL;
    char *profile = NULL;
    bool is_writable = EXTPROTOCOL_VALIDATOR_GET_DIRECTION(fcinfo) == EXT_VALIDATE_WRITE;

    /* Must be called via the external table format manager */
    if (!CALLED_AS_EXTPROTOCOL_VALIDATOR(fcinfo))
        elog(ERROR, "cannot execute pxfprotocol_validate_urls outside protocol manager");

    /*
     * Condition 1: there must be only ONE url.
     */
    if (EXTPROTOCOL_VALIDATOR_GET_NUM_URLS(fcinfo) != 1)
    {
        ereport(ERROR,
                (errcode(ERRCODE_PROTOCOL_VIOLATION),
                        errmsg("number of URLs must be one")));
    }

    uri = parseGPHDUri(EXTPROTOCOL_VALIDATOR_GET_NTH_URL(fcinfo, 1));

    /*
     * Condition 2: check for valid cluster name
     */
    GPHDUri_verify_cluster_exists(uri, "default");

    /*
     * Condition 3: Test that Fragmenter or Profile was specified in the URI
     */
    if (GPHDUri_get_value_for_opt(uri, "fragmenter", &fragmenter, false) != 0
        && GPHDUri_get_value_for_opt(uri, "profile", &profile, false) != 0)
    {
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("FRAGMENTER or PROFILE option must exist in %s", uri->uri)));
    }

    /*
    * Condition 4: No duplicate options.
    */
    GPHDUri_verify_no_duplicate_options(uri);

    /*
     * Condition 5: existence of core options if profile wasn't supplied
     */
    if (GPHDUri_get_value_for_opt(uri, "profile", &option, false) < 0)
    {
        List *coreOptions = list_make2("ACCESSOR", "RESOLVER");
        if (!is_writable)
        {
            coreOptions = lcons("FRAGMENTER", coreOptions);
        }
        GPHDUri_verify_core_options_exist(uri, coreOptions);
        list_free(coreOptions);
    }

    /* Temp: Uncomment for printing a NOTICE with parsed parameters */
    /*   GPHDUri_debug_print(uri); */

    /* if we're here - the URI is valid. Don't need it no more */
    freeGPHDUri(uri);

    PG_RETURN_VOID();
}

/*
 * Writes to an external table
 */
Datum
pxfprotocol_export(PG_FUNCTION_ARGS)
{
    //TODO: provide real implementation
	elog(INFO, "Dummy PXF protocol write");
    PG_RETURN_INT32(0);
}

/*
 * Reads tuples from an external table
 */
Datum
pxfprotocol_import(PG_FUNCTION_ARGS)
{
    /* Must be called via the external table format manager */
    check_caller(fcinfo, "pxfprotocol_import");

    /* retrieve user context */
    gphadoop_context *context = (gphadoop_context *) EXTPROTOCOL_GET_USER_CTX(fcinfo);

    /* last call -- cleanup */
    if (EXTPROTOCOL_IS_LAST_CALL(fcinfo)) {
        cleanup_context(context);
        EXTPROTOCOL_SET_USER_CTX(fcinfo, NULL);
        PG_RETURN_INT32(0);
    }

    /* first call -- do any desired init */
    if (context == NULL) {
        context = create_context(fcinfo, true);
        EXTPROTOCOL_SET_USER_CTX(fcinfo, context);
        gpbridge_import_start(context);
    }

    int bytes_read = gpbridge_read(context, EXTPROTOCOL_GET_DATABUF(fcinfo), EXTPROTOCOL_GET_DATALEN(fcinfo));

    PG_RETURN_INT32(bytes_read);
}

/*
 * Allocates context and initializes values.
 */
static gphadoop_context*
create_context(PG_FUNCTION_ARGS, bool is_import)
{
    gphadoop_context* context = palloc0(sizeof(gphadoop_context));

    //TODO: remove mocking fragment data
    // append mock fragment data to original uri
    //&segwork=<size>@<ip>@<port>@<index><size>@<ip>@<port>@<index><size>
    char *original_uri = EXTPROTOCOL_GET_URL(fcinfo);
    StringInfoData uri_with_segwork;
    initStringInfo(&uri_with_segwork);

    /*
    fragment 1: segwork=46@127.0.0.1@51200@tmp/dummy1.1@0@ZnJhZ21lbnQx@@@
    fragment 2: segwork=46@127.0.0.1@51200@tmp/dummy1.2@0@ZnJhZ21lbnQy@@@
    fragment 3: segwork=46@127.0.0.1@51200@tmp/dummy1.3@0@ZnJhZ21lbnQz@@@
    */

    appendStringInfo(&uri_with_segwork,
                     "%s&segwork=46@127.0.0.1@51200@tmp/dummy1.%d@0@ZnJhZ21lbnQ%c@@@",
                     original_uri,
                     GpIdentity.segindex + 1,
                     'x' + GpIdentity.segindex);

    /* parse the URI */
    context->gphd_uri = parseGPHDUri(uri_with_segwork.data);
    if (is_import)
        Assert(context->gphd_uri->fragments != NULL);

    initStringInfo(&context->uri);
    initStringInfo(&context->write_file_name);
    context->relation = EXTPROTOCOL_GET_RELATION(fcinfo);

    //TODO: remove this when using real fragmentation data
    pfree(uri_with_segwork.data);

    return context;
}

/*
 * De-allocates context and dependent structures.
 */
static void
cleanup_context(gphadoop_context *context)
{
    if (context != NULL)
    {
        gpbridge_cleanup(context);
        pfree(context->uri.data);
        pfree(context->write_file_name.data);
        pfree(context);
    }
}

/*
 * Checks that the caller is External Protocol Manager
 */
static void
check_caller(PG_FUNCTION_ARGS, const char* func_name)
{
    if (!CALLED_AS_EXTPROTOCOL(fcinfo))
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("%s not called by external protocol manager", func_name)));
}
