from theatre.interfaces import System
from theatre.threaded_theatre import curtain_call
import sys
import socket
import logging
import os
import struct

logging.basicConfig(level=logging.INFO, style="{")
SOCKET_PATH = "/tmp/theatre-echo.sock"


def get_peer_cred(conn: socket.socket):
    cred = conn.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i"))
    pid, uid, gid = struct.unpack("3i", cred)
    return pid, uid, gid


def log_adapter(record_adapter):
    class _LoggingAdapter(logging.LoggerAdapter):
        def process(self, msg, kwargs):
            return record_adapter(self.extra, msg, kwargs)
    return _LoggingAdapter


@log_adapter
def contextual_adapter(ctx: dict, msg: str, kwargs: dict):
    context = ctx | kwargs.pop("context", {})
    attrs = " ".join(f"{key}={value}" for key, value in context.items())
    return f"{msg} [{attrs}]", kwargs


logger = contextual_adapter(logging.getLogger(__name__), {})

def session_handler(conn, logger):
    try:
        (pid, uid, gid) = get_peer_cred(conn)
        logger = contextual_adapter(logger, dict(
            peer_pid=pid, peer_uid=uid, peer_gid=gid,
            actor_address=(yield System.whoami())
        ))
        while True:
            logger.info("Waiting for data from client")
            try:
                data = yield System.call(conn.recv, (1024,))
            except socket.timeout:
                continue
            else:
                logger.info("Received %d bytes of data from client", len(data))
                logger.debug("Received: %s", data)
                if not data:
                    logger.info("Terminating session")
                    return
                yield System.call(conn.send, (data,))
    finally:
        conn.close()


def listener():
    _logger = contextual_adapter(logger, dict(
        listener_address=(yield System.whoami())
    ))
    try:
        os.unlink(SOCKET_PATH)
    except FileNotFoundError:
        pass
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(SOCKET_PATH)
    server.listen(5)
    server.settimeout(1.0) # blocking accept
    sessions = {}
    _logger.info(f"Listening to unix:{SOCKET_PATH}", context=dict(
        socket_path=SOCKET_PATH
    ))
    while True:
        try:
            conn, _ = yield System.call(server.accept)
            _logger.info("Received client connection")
            session_id = id(conn)
            session_logger = contextual_adapter(
                logging.getLogger(f"{__name__}.session.{session_id}"),
                dict(
                    session_id=session_id
                )
            )
            session = yield System.spawn(session_handler, (conn, session_logger))
            _logger.info(f"Spawned actor {session} to handle client session")
            sessions[session] = conn
        except socket.timeout:
            continue
        except Exception as ex:
            for session, conn in sessions.items():
                yield System.call(conn.close)
                yield System.kill(session, reason=ex)
            raise



def main(*args):
    try:
        with curtain_call(max_idle=5) as theatre:
            theatre.run(listener)
    finally:
        try:
            os.unlink(SOCKET_PATH)
        except FileNotFoundError:
            pass


if __name__ == "__main__":
    main(*sys.argv[0:])

