CLI Helper
----------

In order to speed up command kart command invocations a helper mode 
can be enabled on Linux and MacOS

This mode will start a background process which stays running 
and forks a new process to perform the command so as to 
not include python import time overhead.

The helper command
''''''''''''''''''

``kart helper`` starts the background process and supports the
``--socket`` and ``--timeout`` options. ``--socket`` is the
name of a UNIX socket to be used to communcate between the client
and helper. ``--socket`` defaults to $HOME/.kart.socket and
setting a seperate socket name is not currently supported in the 
client. ``--timout`` specifies how long the helper will wait
for a command from the client before shutting down.

In normal operation ``kart helper`` is started by the client
process which is part of the standard ``kart`` startup process
but the client will use a previously started helper if it can 
connect on the named UNIX socket.

Through an environment variable, ``KART_HELPER_LOG``, it is possible
to enable logging of all commands performed by the helper. This will
log the PID and working directory of the client along with the command
and args that were run to the filename in ``KART_HELPER_LOG``.


The client
''''''''''

Helper mode is enabled by specifying setting and environment variable, 
``KART_USE_HELPER``. 0, not set and any value with leading spaces are 
all treated as false, any other value is true.


Operation
'''''''''

When helper mode is enabled the client, ``kart``, will try to connect
to a UNIX socket, if that is not possible it will try to start the 
helper command of kart and wait until the socket is available.

During starting the helper command the client will double fork, close
file descriptors and execute the helper command so that it is fully 
detached in the background to continue running after this invocation 
of the client. The helper command will start, open a socket and listen
along with importing any expensive python libraries so as to ensure 
no imports are done when a fork is performed to run a command.

Once the client can connect to the socket it creates a semaphore to 
receive the exit code of kart and sends a JSON dictionary of the 
local environment at calling time, command arguments to run, semaphore
ID and PID of the client process. This is sent to the helper along with
stdin, stdout, stderr file descriptors and a file descriptor of the 
current working directory. The client then needs to wait as long as 
the longest kart command might take, currently it waits for 1 day.

On receiving a request from the client the helper mode forks a child,
sets up the environment and working directory as per the client process, 
opens the stdin, stdout, stderr file descriptors from the client and 
assigns them to sys.stdin, sys.stdout, sys.stderr so that output will 
go directly to the calling process standard streams without copying 
then runs the command through ``cli()`` as usual.

Once the command completes the helper writes the exit code to the
semphore and sends a SIGALRM to the client. The client handles the 
SIGALRM by reading the semphore to set the exit code and exiting.



