"""FastAPI jobs must keep a checkable handler signature (PROJECT_SCOPE.md §5, §17).

``Depends()`` parameters stay in the static signature. A direct call passes them
explicitly. ``dispatch`` omits them because they are injected at run time and the
default lets the type checker accept a payload-only call. ``current_job()`` is the
typed way to read the delivery context.
"""

from __future__ import annotations

from fastapi import Depends, FastAPI
from typing_extensions import assert_type

from laravel_cloud_queues import DispatchReceipt, JobContext, current_job
from laravel_cloud_queues.fastapi import LaravelCloudQueues

app = FastAPI()
queues = LaravelCloudQueues(app)


def get_mailer() -> str:
    return "smtp"


@queues.job(name="emails.send")
def send_email(user_id: int, mailer: str = Depends(get_mailer)) -> str:
    context = current_job()
    assert_type(context, JobContext)
    return f"{user_id}:{mailer}"


def direct_and_dispatch() -> None:
    assert_type(send_email(1, mailer="smtp"), str)
    assert_type(send_email.dispatch(1), DispatchReceipt)
