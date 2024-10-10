import array
import socket

# Setting the message length higher than this has no effect - the message gets chunked by TCP anyway.
MAX_CHUNK_LEN = 8164


# Function modified from https://docs.python.org/3/library/socket.html#socket.socket.recvmsg
def recv_json_and_fds(sock, maxfds=0):
    chunks = []
    fds = array.array("i")  # Array of ints
    while True:
        chunk, ancdata, flags, addr = sock.recvmsg(
            MAX_CHUNK_LEN, socket.CMSG_LEN(maxfds * fds.itemsize)
        )
        chunks.append(chunk)
        for cmsg_level, cmsg_type, cmsg_data in ancdata:
            if (
                cmsg_level == socket.SOL_SOCKET
                and cmsg_type == socket.SCM_RIGHTS
                and cmsg_data
            ):
                fds.frombytes(
                    cmsg_data[: len(cmsg_data) - (len(cmsg_data) % fds.itemsize)]
                )
        if not chunk or chunk.rstrip().endswith(b"}"):
            break

    return b"".join(chunks), list(fds)
