
#include <errno.h>
#include <fcntl.h>
#include <fcntl.h>
#include <libgen.h>
#include <limits.h>
#include <locale.h>
#include <signal.h>
#include <spawn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/sem.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#if __APPLE__
#include <mach-o/dyld.h>
const int SEM_FLAGS = IPC_CREAT | IPC_EXCL | SEM_R | SEM_A;
#elif __linux__
#define _GNU_SOURCE
const int SEM_FLAGS = IPC_CREAT | IPC_EXCL | S_IRUSR | S_IWUSR;
#endif


int nanosleep(const struct timespec *req, struct timespec *rem);

#include "cJSON.h"

int semid;
const int SEMNUM = 0;

#ifndef DEBUG
#define DEBUG 0
#endif
#define debug(fmt, ...) \
        do { if (DEBUG && getenv("KART_HELPER_DEBUG") != NULL) { \
            fprintf(stderr, "HELPER[%d]:%d:%s(): " fmt, getpid(), \
                    __LINE__, __func__, ##__VA_ARGS__); }} while (0)

/**
 * @brief find the path to the current executable
 * @param[in] argv process argv
 * @param[out] exe_path path to the executable, absolute or relative to cwd. len=PATH_MAX
 * @return 0 success, 1 error
 */
int find_executable(char **argv, char *exe_path)
{
    exe_path[0] = '\0';

#if __linux__
    ssize_t r = readlink("/proc/self/exe", exe_path, PATH_MAX);
    if(r == -1)
    {
        debug("Error calling readlink(/proc/self/exe): %d\n", errno);
    }
    else
    {
        exe_path[r] = '\0';
        debug("readlink(/proc/self/exe)=%s\n", exe_path);
    }
#elif __APPLE__
    uint32_t bufsize = PATH_MAX;
    int e = _NSGetExecutablePath(exe_path, &bufsize);
    if (e)
    {
        debug("Error calling _NSGetExecutablePath(): %d (bufsize=%d)\n", e, bufsize);
    }
    else
    {
        debug("_NSGetExecutablePath=%s\n", exe_path);
    }
#endif

    // that didn't work for some reason, try argv[0]
    if (!exe_path[0])
    {
        if (!realpath(argv[0], exe_path))
        {
            debug("Error calling realpath(argv[0]=%s)\n", argv[0]);
            return 1;
        }
        debug("realpath(argv[0])=%s\n", exe_path);
    }

    return 0;
}

/**
 * @brief Base64 encode a string to handle invalid UTF-8 sequences
 * @param[in] input Input bytes
 * @param[in] input_len Length of input
 * @param[out] output Output buffer (must be at least ((input_len + 2) / 3) * 4 + 1 bytes)
 * @return Length of encoded string (not including null terminator)
 */
size_t base64_encode(const unsigned char *input, size_t input_len, char *output)
{
    const char base64_chars[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    size_t i, j;
    unsigned char char_array_3[3];
    unsigned char char_array_4[4];

    j = 0;
    for (i = 0; i < input_len; ) {
        char_array_3[0] = i < input_len ? input[i++] : 0;
        char_array_3[1] = i < input_len ? input[i++] : 0;
        char_array_3[2] = i < input_len ? input[i++] : 0;

        char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
        char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
        char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
        char_array_4[3] = char_array_3[2] & 0x3f;

        for(int k = 0; k < 4; k++)
            output[j++] = base64_chars[char_array_4[k]];
    }

    // Add padding
    size_t mod = input_len % 3;
    if (mod == 1) {
        output[j - 2] = '=';
        output[j - 1] = '=';
    } else if (mod == 2) {
        output[j - 1] = '=';
    }

    output[j] = '\0';
    return j;
}

/**
 * @brief find the path to the kart_cli executable
 * @param[in] source absolute or relative path to existing file
 * @param[in] name sibling filename
 * @param[out] sibling_path sibling path name. len=PATH_MAX
 * @return 0 success, 1 error
 */
int find_sibling(char* source, char* name, char* sibling_path)
{
    (void)memset(sibling_path, 0, PATH_MAX);

    // look for a sibling of the executable
    char *p = strrchr(source, '/');
    if(p == NULL)
    {
        (void)strncpy(sibling_path, name, PATH_MAX-1);
    }
    else
    {
        char buf[PATH_MAX];
        if (snprintf(buf, PATH_MAX, "/%s", name) < 0)
        {
            fprintf(stderr, "Error calculating sibling path\n");
            return 1;
        }

        (void)strncpy(sibling_path, source, p - source);
        (void)strncat(sibling_path, buf, PATH_MAX - strlen(buf) - 1);
    }
    debug("sibling path: %s\n", sibling_path);

    return 0;
}

/**
 * @brief find the path to the kart_cli executable
 * @param[in] argv process argv
 * @param[out] exe_path path to the executable, absolute or relative to cwd. len=PATH_MAX
 * @return 0 success, 1 error
 */
int find_kart_cli(char **argv, char *cmd_path)
{
    char exe_path[PATH_MAX];
    int r;
    r = find_executable(argv, exe_path);
    if (r)
    {
        return r;
    }
    debug("executable=%s\n", exe_path);

    r = find_sibling(exe_path, "kart_cli", cmd_path);
    if (r)
    {
        return r;
    }

    if (access(cmd_path, F_OK) == 0) {
        // found it
        return 0;
    }

    // file doesn't exist
    debug("%s doesn't exist\n", cmd_path);

    // if kart is a symlink, try resolving it then finding the symlink
    char buf[PATH_MAX];
    if (!realpath(exe_path, buf))
    {
        fprintf(stderr, "Error resolving kart_cli path\n");
        return 1;
    }
    debug("realpath(%s)=%s\n", exe_path, buf);

    r = find_sibling(buf, "kart_cli", cmd_path);
    if (r)
    {
        return r;
    }

    if (access(cmd_path, F_OK) == 0) {
        // found it
        return 0;
    }

    debug("%s doesn't exist\n", cmd_path);
    return 1;
}

/**
 * @brief Check whether helper is enabled via KART_USE_HELPER
 * Defaults on, turn off via KART_USE_HELPER=0
 * @return 0 no, 1 yes
 */
int is_helper_enabled()
{
    char *env = getenv("KART_USE_HELPER");
    return (env == NULL || *env != '0');
}

/**
 * @brief Exit signal handler for SIGALRM
 */
void exit_on_sigalrm(int sig)
{
    int semval = semctl(semid, SEMNUM, GETVAL);
    if (semval < 0)
    {
        debug("sigalrm: error getting semval, semid=%d, errno=%d\n", semid, errno);
        exit(5);
    }

    int exit_code = semval - 1000;
    semctl(semid, SEMNUM, IPC_RMID);
    debug("sigalrm: semid=%d semval=%d exit_code=%d\n", semid, semval, exit_code);
    exit(exit_code);
}

/**
 * @brief Exit signal handler for SIGINT.
 * Tries to kill the whole process group.
 */
void exit_on_sigint(int sig)
{
    putchar('\n');
    killpg(0, sig);
    exit(128 + sig);
}

void handle_sigusr1(int sig) {
    // This lets the child signal to us that we shouldn't try to kill kart when SIGINT (Ctrl+C) occurs.
    signal(SIGINT, SIG_IGN);
    sleep(86400);
}

int main(int argc, char **argv, char **environ)
{
    char cmd_path[PATH_MAX];
    if (find_kart_cli(argv, cmd_path)) {
        fprintf(stderr, "Couldn't find kart_cli\n");
        exit(1);
    }

    if (is_helper_enabled())
    {
        debug("enabled %s, pid=%d\n", cmd_path, getpid());

        // Make this process the leader of a process group:
        // The procress-group ID (pgid) will be the same as the pid.
        setpgrp();

        // start or use an existing helper process
        char **env_ptr;

        int listSZ;
        for (listSZ = 0; environ[listSZ] != NULL; listSZ++)
            ;
        char **helper_environ = malloc((listSZ + 1) * sizeof(char *));
        if (!helper_environ) {
            fprintf(stderr, "kart: failed to allocate memory for environment\n");
            return 1;
        }

        cJSON *env = NULL;
        cJSON *args = NULL;
        cJSON *payload = cJSON_CreateObject();
        cJSON_AddNumberToObject(payload, "pid", getpid());
        env = cJSON_AddObjectToObject(payload, "environ");

        int found = 0;
        // filter the environment so that KART_USE_HELPER isn't passed to the
        // spawned process and so getting into a loop
        for (env_ptr = environ; *env_ptr != NULL; env_ptr++)
        {
            char *key = malloc(strlen(*env_ptr) + 1);
            if (!key) {
                fprintf(stderr, "kart: failed to allocate memory for environment variable\n");
                free(helper_environ);
                cJSON_Delete(payload);
                return 1;
            }

            char *val = malloc(strlen(*env_ptr) + 1);
            if (!val) {
                fprintf(stderr, "kart: failed to allocate memory for environment variable\n");
                free(key);
                free(helper_environ);
                cJSON_Delete(payload);
                return 1;
            }

            if (sscanf(*env_ptr, "%[^=]=%[^\x04]", key, val) != 2) {
                // not found with two values in a key=value pair
                if (sscanf(*env_ptr, "%[^=]=", key) != 1) {
                    fprintf(stderr, "error reading environment variable where only name is present\n");
                }
                val[0] = '\0';  // Empty value
            }
            if (strcmp(key, "KART_USE_HELPER"))
            {
                helper_environ[found++] = *env_ptr;

                // Base64 encode the value to handle invalid UTF-8 sequences
                size_t val_len = strlen(val);
                size_t encoded_len = ((val_len + 2) / 3) * 4 + 1;
                char *encoded_val = malloc(encoded_len);
                if (!encoded_val) {
                    fprintf(stderr, "kart: failed to allocate memory for Base64 encoding\n");
                    free(key);
                    free(val);
                    free(helper_environ);
                    cJSON_Delete(payload);
                    return 1;
                }
                base64_encode((unsigned char*)val, val_len, encoded_val);

                cJSON_AddStringToObject(env, key, encoded_val);
                free(encoded_val);
            }
            free(key);
            free(val);
        }
        helper_environ[listSZ] = NULL;

        char **arg_ptr;
        args = cJSON_AddArrayToObject(payload, "argv");
        for (arg_ptr = argv; *arg_ptr != NULL; arg_ptr++)
        {
            cJSON_AddItemToArray(args, cJSON_CreateString(*arg_ptr));
        }

        int fp = open(getcwd(NULL, 0), O_RDONLY);
        int fds[4] = {fileno(stdin), fileno(stdout), fileno(stderr), fp};

        size_t socket_filename_sz = strlen(getenv("HOME")) + strlen("/.kart..socket") + sizeof(pid_t) * 3 + 1;
        char *socket_filename = malloc(socket_filename_sz);
        if (!socket_filename) {
            fprintf(stderr, "kart: failed to allocate memory for socket filename\n");
            free(helper_environ);
            cJSON_Delete(payload);
            close(fp);
            return 1;
        }
        int r = snprintf(socket_filename, socket_filename_sz, "%s/.kart.%d.socket", getenv("HOME"), getsid(0));
        if (r < 0 || (size_t) r >= socket_filename_sz)
        {
            fprintf(stderr, "Error allocating socket filename\n");
            free(socket_filename);
            free(helper_environ);
            cJSON_Delete(payload);
            close(fp);
            return 1;
        }

        int socket_fd = socket(AF_UNIX, SOCK_STREAM, 0);

        struct sockaddr_un addr;
        addr.sun_family = AF_UNIX;
        strcpy(addr.sun_path, socket_filename);

        // if there is no open socket perform a double fork and spawn to
        // detach the helper, wait till the first forked child has completed
        // then attempt to connect to the socket the helper will open
        if (connect(socket_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0)
        {
            debug("no open socket found @%s\n", socket_filename);
            int status;
            if (fork() == 0)
            {
                // create a grandchild process and close stdin/stdout/stderr
                // to detach the helper process and ensure no fd's from the initial calling
                // process are left open in it
                if (fork() == 0)
                {
                    // start helper in background and wait
                    char *helper_argv[] = {&cmd_path[0], "helper", "--socket", socket_filename, NULL};

                    int status;
                    environ = helper_environ;
                    for (int fd = 0; fd < 3; fd++){
                        fcntl(fd, F_SETFD, FD_CLOEXEC);
                    }
                    debug("grandchild: execvp: %s helper --socket %s\n", cmd_path, socket_filename);
                    status = execvp(helper_argv[0], helper_argv);

                    if (status < 0)
                    {
                        fprintf(stderr, "Error running kart helper, %s: %s", cmd_path, strerror(status));
                        exit(1);
                    }
                }
                exit(0);
            }
            else
            {
                wait(&status);
            }

            debug("parent: waiting for socket\n");

            int rtc, max_retry = 50;
            struct timespec sleep_for = {0, 250 * 1000 * 1000};
            while ((rtc = connect(socket_fd, (struct sockaddr *)&addr, sizeof addr)) != 0 && --max_retry >= 0)
            {
                nanosleep(&sleep_for, NULL);
            }
            if (rtc < 0)
            {
                fprintf(stderr, "Timeout connecting to kart helper\n");
                return 2;
            }
        } else {
            debug("open socket found @%s\n", socket_filename);
        }

        // set up exit code semaphore
        if ((semid = semget(IPC_PRIVATE, 1, SEM_FLAGS)) < 0)
        {
            fprintf(stderr, "Error setting up result communication with helper %s\n", strerror(errno));
            return 5;
        }

        cJSON_AddNumberToObject(payload, "semid", semid);
        char *payload_string = cJSON_PrintUnformatted(payload);

        debug("payload (%lub): %s\n", strlen(payload_string), payload_string);

        struct iovec iov = {
            .iov_base = payload_string,
            .iov_len = strlen(payload_string)};

        union
        {
            char buf[CMSG_SPACE(sizeof(fds))];
            struct cmsghdr align;
        } u;

        struct msghdr msg = {
            .msg_iov = &iov,
            .msg_iovlen = 1,
            .msg_control = u.buf,
            .msg_controllen = sizeof(u.buf)};

        struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);

        *cmsg = (struct cmsghdr){
            .cmsg_level = SOL_SOCKET,
            .cmsg_type = SCM_RIGHTS,
            .cmsg_len = CMSG_LEN(sizeof(fds))};

        memcpy((int *)CMSG_DATA(cmsg), fds, sizeof(fds));
        msg.msg_controllen = cmsg->cmsg_len;

        signal(SIGALRM, exit_on_sigalrm);
        signal(SIGINT, exit_on_sigint);
        signal(SIGUSR1, handle_sigusr1);

        if (sendmsg(socket_fd, &msg, 0) < 0)
        {
            fprintf(stderr, "Error sending command to kart helper %s\n", strerror(errno));
            return 3;
        };

        debug("complete, sleeping until exit\n");

        // The process needs to sleep for as long as the longest command, clone etc. could take.
        sleep(86400);
        fprintf(stderr, "Timed out, no response from kart helper\n");
        return 4;
    }
    else
    {
        debug("disabled, execvp(%s)\n", cmd_path);
        // run the full application as normal
        execvp(&cmd_path[0], argv);
    }
}
