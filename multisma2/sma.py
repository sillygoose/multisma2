"""SMA WebConnect library for Python.
     See: http://www.sma.de/en/products/monitoring-control/webconnect.html
Based on: http://www.github.com/kellerza/pysma
"""

import asyncio
import json
import logging

import async_timeout
import jmespath
from aiohttp import client_exceptions

from exceptions import SmaException


_LOGGER = logging.getLogger('multisma2')

USERS = {'user': 'usr', 'installer': 'istl'}

JMESPATH_BASE = 'result.*'
JMESPATH_VAL_IDX = '"1"[{}].val'
JMESPATH_VAL = 'val'

URL_LOGIN = '/dyn/login.json'
URL_LOGOUT = '/dyn/logout.json'
URL_VALUES = '/dyn/getValues.json'
URL_LOGGER = '/dyn/getLogger.json'
URL_ONLINE = '/dyn/getAllOnlValues.json'


class SMA:
    """Class to connect to the SMA webconnect module and read parameters."""

    def __init__(self, session, url, password, group='user', uid=None):
        """Init SMA connection."""
        if group not in USERS:
            _LOGGER.debug(f"Invalid user type: {group}")
            raise SmaException(SmaException.BAD_USER_TYPE)
        if password is not None and len(password) > 12:
            _LOGGER.debug("Password should not exceed 12 characters")
            raise SmaException(SmaException.PASSWORD_TOO_LONG)
        if password is None or len(password) == 0:
            self._new_session_data = None
            _LOGGER.debug("Password is required to login the inverter")
            raise SmaException(SmaException.PASSWORD_REQUIRED)
        else:
            self._new_session_data = {'right': USERS[group], 'pass': password}
        self._url = url.rstrip('/')
        if not url.startswith('http'):
            self._url = 'http://' + self._url
        self._aio_session = session
        self.sma_sid = None
        self.sma_uid = uid

    async def _fetch_json(self, url, payload):
        """Fetch json data for requests."""
        params = {
            'data': json.dumps(payload),
            'headers': {'content-type': 'application/json'},
            'params': {'sid': self.sma_sid} if self.sma_sid else None,
        }
        for _ in range(3):
            try:
                with async_timeout.timeout(3):
                    res = await self._aio_session.post(self._url + url, **params)
                    return (await res.json()) or {}
            except (asyncio.TimeoutError, client_exceptions.ClientError):
                continue
        return {'err': f"Could not connect to SMA at {self._url} (timeout)"}

    async def _read_body(self, url, payload):
        if self.sma_sid is None and self._new_session_data is not None:
            await self.new_session()
            if self.sma_sid is None:
                _LOGGER.debug(f"Unable to create new session with inverter {self._url}")
                raise SmaException(SmaException.NO_SESSION)

        body = await self._fetch_json(url, payload=payload)

        # On the first error we close the session which will re-login
        err = body.get('err')
        if err is not None:
            _LOGGER.debug(
                f"{self._url}: error detected, closing session to force another login attempt, got: {body}",
            )
            await self.close_session()
            raise SmaException(SmaException.ERR_RETURNED)

        if not isinstance(body, dict) or 'result' not in body:
            _LOGGER.debug(f"No 'result' in reply from SMA, got: {body}")
            raise SmaException(SmaException.NO_RESULT)

        if self.sma_uid is None:
            # Get the unique ID
            self.sma_uid = next(iter(body['result'].keys()), None)

        result_body = body['result'].pop(self.sma_uid, None)
        if body != {'result': {}}:
            _LOGGER.debug(f"Unexpected body {json.dumps(body)}, extracted {json.dumps(result_body)}")
            raise SmaException(SmaException.UNEXPECTED_BODY)

        return result_body

    async def new_session(self) -> None:
        """Establish a new session."""
        body = await self._fetch_json(URL_LOGIN, self._new_session_data)
        self.sma_sid = jmespath.search('result.sid', body)
        if self.sma_sid:
            return

        err = body.pop('err', None)
        msg = f"Could not start session, {body}, got {{}}"
        if err:
            if err == 503:
                _LOGGER.debug(msg, "Max amount of sessions reached")
                raise SmaException(SmaException.MAX_SESSIONS)
            _LOGGER.debug(msg, err)
            raise SmaException(SmaException.START_SESSION, msg)
        else:
            _LOGGER.debug(msg, "Session ID expected [result.sid]")
            raise SmaException(SmaException.SESSION_ID_EXPECTED)

    async def close_session(self):
        """Close the session."""
        if self.sma_sid is not None:
            try:
                await self._fetch_json(URL_LOGOUT, {})
            finally:
                self.sma_sid = None

    async def read_values(self, keys):
        """Read a list of one or more keys."""
        payload = {'destDev': [], 'keys': keys}
        result_body = await self._read_body(URL_VALUES, payload)
        return result_body

    async def read_instantaneous(self):
        """One command to read the sensors in the Instantaneous inverter view."""
        payload = {'destDev': []}
        result_body = await self._read_body(URL_ONLINE, payload)
        return result_body

    async def read_history(self, start, end):
        """Read the history for the specified period."""
        # {'destDev':[],'key':28704,'tStart':1601521200,'tEnd':1604217600}.
        payload = {'destDev': [], 'key': 28704, 'tStart': start, 'tEnd': end}
        result_body = await self._read_body(URL_LOGGER, payload)
        return result_body

    async def read_fine_history(self, start, end):
        """Read the fine history for the specified period."""
        # {'destDev':[],'key':28672,'tStart':1601521200,'tEnd':1604217600}.
        payload = {'destDev': [], 'key': 28672, 'tStart': start, 'tEnd': end}
        result_body = await self._read_body(URL_LOGGER, payload)
        return result_body
