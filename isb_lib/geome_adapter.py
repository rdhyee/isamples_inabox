"""
Implements an adapter for getting records from GEOME
"""

import logging
import datetime
import requests
import sickle.oaiexceptions
import sickle.utils
import igsn_lib.oai
import igsn_lib.time
import isb_lib.core

HTTP_TIMEOUT = 10.0  # seconds
GEOME_API = "https://api.geome-db.org/v1/"


def getLogger():
    return logging.getLogger("isb_lib.geome_adapter")


class GEOMEIdentifierIterator(isb_lib.core.IdentifierIterator):
    def __init__(
        self,
        offset: int = 0,
        max_entries: int = -1,
        date_start: datetime.datetime = None,
        date_end: datetime.datetime = None,
    ):
        super().__init__(
            offset=offset,
            max_entries=max_entries,
            date_start=date_start,
            date_end=date_end,
        )
        self._project_ids = None

    def listProjects(self):
        L = getLogger()
        L.debug("Loading project ids...")
        url = f"{GEOME_API}projects"
        headers = {
            "Accept": "application/json",
        }
        params = {"includePublic": "true", "admin": "false"}
        response = requests.get(
            url, params=params, headers=headers, timeout=HTTP_TIMEOUT
        )
        if response.status_code != 200:
            raise ValueError(
                "Unable to load projects, status: %s; reason: %s",
                response.status_code,
                response.reason,
            )
        projects = response.json()
        for project in projects:
            yield project

    def recordsInProject(self, project_id, record_type="Event"):
        L = getLogger()
        L.debug("Loading identifiers for project %s", project_id)
        url = f"{GEOME_API}records/Sample/json"
        headers = {
            "Accept": "application/json",
        }
        _page_size = 1000
        params = {
            "limit": _page_size,
            "page": 0,
            "q": f"_projects_:{project_id}",
        }
        identifiers = []
        more_work = True
        while more_work:
            response = requests.get(
                url, params=params, headers=headers, timeout=HTTP_TIMEOUT
            )
            if response.status_code != 200:
                raise ValueError(
                    "Unable to load records, status: %s; reason: %s",
                    response.status_code,
                    response.reason,
                )
            data = response.json()
            for record in data.get("content", {}).get(record_type, []):
                yield record
            if len(data.get("content", {}).get(record_type, [])) < _page_size:
                more_work = False
            params["page"] = params["page"] + 1
        L.debug("Found %s identifiers in project_id %s", len(identifiers), project_id)
        return identifiers

    def _loadEntries(self):
        if self._project_ids is None:
            # Load the project IDs
            # each entry is a dict with key
            self._project_ids = []
            for p in self.listProjects():
                pid = p.get("projectId", None)
                if not pid is None:
                    self._project_ids.append(
                        {
                            "project_id": pid,
                            "identifiers": [],
                            "loaded": False,
                        }
                    )
        for p in self._project_ids:
            #return the next set of identifiers within a project
            if not p["loaded"]:
                for record in self.recordsInProject(p['project_id'], record_type='Event'):
                    rid = record.get('bcid', None)
                    if not rid is None:
                        p['identifiers'].append(rid)
                p['loaded'] = True
                self._cpage = p['identifiers']
                self._total_records += len(self._cpage)
                break
        self._cpage = []

    def __len__(self):
        return self._total_records

    def _getPage(self):
        if self._cpage is None:
            self._loadEntries()
        if self._coffset >= self._total_records:
            return
        self._page_offset = 0
