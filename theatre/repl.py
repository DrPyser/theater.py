import logging
import argparse
import code
from theatre.threaded_theatre import (
    Theatre,
    curtain_call,
    Signal,
    NormalExit,
    ErrorExit,
    ActorTerminated,
    ActorCancelled,
    MailboxFull,
    ReceiveTimeout,
    RequestCancelled,
    DestinationNotFound,
    UnsupportedRequest,
    ActorSignaled,
)
from theatre.interfaces import System


def printer(output=None):
    while msg := (yield System.receive()):
        print(msg, file=output)


def repl(**kwargs):
    with curtain_call(**kwargs) as t:
        ns = dict(
            t=t,
            theatre=t,
            spawn=t.spawn,
            send=t.send,
            spotlight=t.spotlight,
            wait_ensemble=t.wait_ensemble,
            cancel=t.cancel,
            kill=t.kill,
            signal=t.signal,
            signal_all=t.signal_all,
            System=System,
            Signal=Signal,
            NormalExit=NormalExit,
            ErrorExit=ErrorExit,
            ActorTerminated=ActorTerminated,
            ActorCancelled=ActorCancelled,
            MailboxFull=MailboxFull,
            ReceiveTimeout=ReceiveTimeout,
            RequestCancelled=RequestCancelled,
            DestinationNotFound=DestinationNotFound,
            UnsupportedRequest=UnsupportedRequest,
            ActorSignaled=ActorSignaled,
            printer=printer,
        )
        code.interact(banner="Theatre REPL", local=ns)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--queue-size", type=int, default=1024)
    p.add_argument("--clock-tick", type=float, default=1)
    p.add_argument("--max-idle", type=int, default=None)
    p.add_argument("--workers", type=int, default=None)
    args = p.parse_args()
    kw = dict(
        queue_size=args.queue_size, clock_tick=args.clock_tick, max_idle=args.max_idle
    )
    if args.workers:
        from concurrent.futures import ThreadPoolExecutor

        kw["executor"] = ThreadPoolExecutor(max_workers=args.workers)

    logging.basicConfig(level=logging.INFO)
    repl(**kw)
