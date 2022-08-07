
#define _XOPEN_SOURCE 1

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <locale.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <spawn.h>
#include <sys/socket.h>
#include <signal.h>
#include <sys/un.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/sem.h>
#include <errno.h>
#include <time.h>
#include <limits.h>
#include <fcntl.h>
#include <libgen.h>

int nanosleep(const struct timespec *req, struct timespec *rem);

#include "cJSON.h"

int semid;

void exit_on_alarm(int sig)
{
    int semval = semctl(semid, 0, GETVAL);
    int exit_code = semval - 1000;
    semctl(semid, 0, IPC_RMID);
    exit(exit_code);
}

int main(int argc, char **argv, char **environ)
{
    // find the appropriate kart_cli to run based on whether argv[0]
    // is absolute, relative or bare, meaning on the path
    char *cmd = "kart_cli";
    char cmd_path[PATH_MAX];
    char buf[PATH_MAX];
    if (strchr(argv[0], '/')) {
        realpath(argv[0], buf);
        sprintf(cmd_path, "%s/%s", dirname(&buf[0]), cmd);
    } else {
        strcpy(cmd_path, cmd);
    }

    char *use_helper = getenv("KART_USE_HELPER");

    if (use_helper != NULL && *use_helper != '\0' && *use_helper != ' ' && *use_helper != '0')
    {
        // start or use an existing helper process
        char **env_ptr;

        int listSZ;
        for (listSZ = 0; environ[listSZ] != NULL; listSZ++)
            ;
        char **helper_environ = malloc(listSZ * sizeof(char *));

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
            char *key = malloc(strlen(*env_ptr));
            char *val = malloc(strlen(*env_ptr));

            if (sscanf(*env_ptr, "%[^=]=%s", key, val) != 2) {
                // not found with two values in a key=value pair
                if (sscanf(*env_ptr, "%[^=]=", key) != 1) {
                    printf("error reading environment variable where only name is present\n");
                }
            }
            if (strcmp(key, "KART_USE_HELPER"))
            {
                helper_environ[found++] = *env_ptr;
                cJSON_AddStringToObject(env, key, val);
            }
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

        char *socket_filename = malloc(strlen(getenv("HOME")) + strlen(".kart.socket") + 2);
        sprintf(socket_filename, "%s/%s", getenv("HOME"), ".kart.socket");
        int socket_fd = socket(AF_UNIX, SOCK_STREAM, 0);

        struct sockaddr_un addr;
        addr.sun_family = AF_UNIX;
        strcpy(addr.sun_path, socket_filename);

        // if there is no open socket perform a double fork and spawn to 
        // detach the helper, wait till the first forked child has completed
        // then attempt to connect to the socket the helper will open
        if (connect(socket_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0)
        {
            int status;
            if (fork() == 0)
            {
                // create a grandchild process and close stdin/stdout/stderr
                // to detach the helper process and ensure no fd's from the initial calling
                // process are left open in it
                if (fork() == 0)
                {
                    setsid();
                    
                    // start helper in background and wait
                    char *helper_argv[] = {&cmd_path[0], "helper", "--socket", socket_filename, NULL};

                    int status;
                    environ = helper_environ;
                    for (int fd = 0; fd < 3; fd++){
                        fcntl(fd, F_SETFD, FD_CLOEXEC);
                    }
                    status = execvp(helper_argv[0], helper_argv);

                    if (status < 0)
                    {
                        printf("Error running kart helper, %s: %s", cmd_path, strerror(status));
                        exit(1);
                    }
                }
                exit(0);
            }
            else
            {
                wait(&status);
            }

            int rtc, max_retry = 50;
            struct timespec sleep_for = {0, 250 * 1000 * 1000};
            while ((rtc = connect(socket_fd, (struct sockaddr *)&addr, sizeof addr)) != 0 && --max_retry >= 0)
            {
                nanosleep(&sleep_for, NULL);
            }
            if (rtc < 0)
            {
                printf("Timeout connecting to kart helper\n");
                return 2;
            }
        }

        // set up exit code semaphore
        if ((semid = semget(IPC_PRIVATE, 1, IPC_CREAT | IPC_EXCL | S_IRUSR | S_IWUSR)) < 0)
        {
            printf("Error setting up result communication with helper %s\n", strerror(errno));
            return 5;
        };

        cJSON_AddNumberToObject(payload, "semid", semid);
        char *payload_string = cJSON_Print(payload);

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

        signal(SIGALRM, exit_on_alarm);

        if (sendmsg(socket_fd, &msg, 0) < 0)
        {
            printf("Error sending command to kart helper %s\n", strerror(errno));
            return 3;
        }; 
        
        // The process needs to sleep for as long as the longest command, clone etc. could take.
        sleep(86400); 
        printf("Timed out, no response from kart helper\n");
        return 4;
    }
    else
    {
        // run the full application as normal
        execvp(cmd, argv);
    }
}