/* mock implementation for pxfuriparser.h */
GPHDUri*
parseGPHDUri(const char* uri_str)
{
    check_expected(uri_str);
    return (GPHDUri	*) mock();
}

void
freeGPHDUri(GPHDUri* uri)
{
    check_expected(uri);
    mock();
}

int
GPHDUri_opt_exists(GPHDUri *uri, char *key)
{
    check_expected(uri);
    check_expected(key);
    return (int) mock();
}

void
GPHDUri_verify_no_duplicate_options(GPHDUri *uri)
{
    check_expected(uri);
    mock();
}

void
GPHDUri_verify_core_options_exist(GPHDUri *uri, List *coreoptions)
{
    check_expected(uri);
    check_expected(coreoptions);
    mock();
}

void
GPHDUri_verify_cluster_exists(GPHDUri *uri, char* cluster)
{
    check_expected(uri);
    check_expected(cluster);
    mock();
}