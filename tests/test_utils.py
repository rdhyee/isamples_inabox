import datetime
import os
from urllib.request import url2pathname

import requests
from sqlmodel import Session

from isb_lib.models.thing import Thing
from isb_web.sqlmodel_database import insert_identifiers
from typing import Optional


def _add_some_things(
    session: Session,
    num_things: int,
    authority_id: str,
    tcreated: Optional[datetime.datetime] = None,
):
    for i in range(num_things):
        new_thing = Thing(
            id=str(i),
            authority_id=authority_id,
            resolved_url="http://foo.bar",
            resolved_status=200,
            resolved_content={"foo": "bar"},
        )
        if tcreated is not None:
            new_thing.tcreated = tcreated
        insert_identifiers(new_thing)
        session.add(new_thing)
    session.commit()


# Taken from https://stackoverflow.com/questions/10123929/fetch-a-file-from-a-local-url-with-python-requests#22989322
class LocalFileAdapter(requests.adapters.BaseAdapter):
    """Protocol Adapter to allow Requests to GET file:// URLs"""

    @staticmethod
    def _chkpath(method, path):
        """Return an HTTP status for the given filesystem path."""
        if method.lower() in ("put", "delete"):
            return 501, "Not Implemented"  # TODO
        elif method.lower() not in ("get", "head"):
            return 405, "Method Not Allowed"
        elif os.path.isdir(path):
            return 400, "Path Not A File"
        elif not os.path.isfile(path):
            return 404, "File Not Found"
        elif not os.access(path, os.R_OK):
            return 403, "Access Denied"
        else:
            return 200, "OK"

    def send(self, req, **kwargs):  # pylint: disable=unused-argument
        """Return the file specified by the given request

        @type req: C{PreparedRequest}
        @todo: Should I bother filling `response.headers` and processing
               If-Modified-Since and friends using `os.stat`?
        """
        path = os.path.normcase(os.path.normpath(url2pathname(req.path_url)))
        response = requests.Response()

        response.status_code, response.reason = self._chkpath(req.method, path)
        if response.status_code == 200 and req.method.lower() != "head":
            try:
                response.raw = open(path, "rb")
            except (OSError, IOError) as err:
                response.status_code = 500
                response.reason = str(err)

        if isinstance(req.url, bytes):
            response.url = req.url.decode("utf-8")
        else:
            response.url = req.url

        response.request = req
        response.connection = self

        return response

    def close(self):
        pass
