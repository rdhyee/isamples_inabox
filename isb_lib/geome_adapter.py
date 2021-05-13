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

    def _listProjects(self):
        L = getLogger()
        L.debug("Loading project ids...")
        url = f"{GEOME_API}projects"
        headers = {
            "Accept": "application/json",
        }
        params = {"includePublic": "true", "admin": "false"}
        response = requests.get(url, params=params, headers=headers, timeout=HTTP_TIMEOUT)
        if response.status_code != 200:
            raise ValueError(
                "Unable to load projects, status: %s; reason: %s",
                response.status_code,
                response.reason,
            )
        projects = response.json()
        project_ids = []
        for project in projects:
            proj_id = project.get("projectId", None)
            if not proj_id is None:
                project_ids.append(proj_id)
        L.debug("Found %s project Ids", len(project_ids))
        return project_ids

    def _identifiesInProject(self, project_id, record_type="Sample"):
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
            response = requests.get(url, params=params, headers=headers, timeout=HTTP_TIMEOUT)
            if response.status_code == 200:
                data = response.json()
                recs = data.get("content", {}).get("Sample", [])
                nrecs = len(recs)
                for rec in recs:
                    _id = rec.get("bcid", None)
                    if not _id is None:
                        identifiers.append(_id)
                        L.debug(_id)
                if nrecs < _page_size:
                    more_work = False
                params["page"] = params["page"] + 1
            else:
                L.error("Unable to continue on project_id: %s; reason: %s", project_id, response.reason)
                more_work = False
        L.debug("Found %s identifiers in project_id %s", len(identifiers), project_id)
        return identifiers


    def _loadEntries(self):
        _project_ids = self._listProjects()
        self._cpage = []
        for project_id in _project_ids:
            self._cpage += self._identifiesInProject(project_id)
        self._cpage = []
        counter = 0

    def __len__(self):
        return self._total_records

    def _getPage(self):
        if self._cpage is None:
            self._loadEntries()
        if self._coffset >= self._total_records:
            return
        self._page_offset = 0
