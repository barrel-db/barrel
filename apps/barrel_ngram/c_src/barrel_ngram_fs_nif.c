/* Directory fsync for barrel_ngram_fs: Erlang's file module cannot open a
 * directory, so the rename that commits a file is made durable here. */
#include <erl_nif.h>
#include <errno.h>
#include <string.h>
#ifdef _WIN32
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

#define PATH_BUF 4096

static ERL_NIF_TERM errno_term(ErlNifEnv *env, int err)
{
    switch (err) {
    case ENOENT: return enif_make_atom(env, "enoent");
    case ENOTDIR: return enif_make_atom(env, "enotdir");
    case EACCES: return enif_make_atom(env, "eacces");
    case EIO: return enif_make_atom(env, "eio");
    case EINVAL: return enif_make_atom(env, "einval");
    case EBADF: return enif_make_atom(env, "ebadf");
    case EROFS: return enif_make_atom(env, "erofs");
    default: return enif_make_tuple2(env, enif_make_atom(env, "errno"), enif_make_int(env, err));
    }
}

static ERL_NIF_TERM mk_error(ErlNifEnv *env, int err)
{
    return enif_make_tuple2(env, enif_make_atom(env, "error"), errno_term(env, err));
}

static ERL_NIF_TERM fsync_dir_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary bin;
    char path[PATH_BUF];
    (void)argc;
    if (!enif_inspect_iolist_as_binary(env, argv[0], &bin) || bin.size == 0 ||
        bin.size >= PATH_BUF || memchr(bin.data, 0, bin.size) != NULL)
        return enif_make_badarg(env);
    memcpy(path, bin.data, bin.size);
    path[bin.size] = '\0';
#ifdef _WIN32
    /* No flush: NTFS journals rename metadata, and an unprivileged process
     * cannot flush a directory (FlushFileBuffers needs write access to the
     * directory handle). Only check that the directory exists. */
    wchar_t wpath[PATH_BUF];
    DWORD attrs;
    if (MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, path, -1, wpath, PATH_BUF) == 0)
        return enif_make_badarg(env);
    attrs = GetFileAttributesW(wpath);
    if (attrs == INVALID_FILE_ATTRIBUTES)
        return mk_error(env, ENOENT);
    if (!(attrs & FILE_ATTRIBUTE_DIRECTORY))
        return mk_error(env, ENOTDIR);
#else
    int fd, flags = O_RDONLY;
#ifdef O_DIRECTORY
    flags |= O_DIRECTORY;
#endif
    fd = open(path, flags);
    if (fd < 0)
        return mk_error(env, errno);
    if (fsync(fd) != 0) {
        int err = errno;
        close(fd);
        return mk_error(env, err);
    }
    close(fd);
#endif
    return enif_make_atom(env, "ok");
}

static ErlNifFunc nif_funcs[] = {
    {"fsync_dir_nif", 1, fsync_dir_nif, ERL_NIF_DIRTY_JOB_IO_BOUND}};

ERL_NIF_INIT(barrel_ngram_fs, nif_funcs, NULL, NULL, NULL, NULL)
