"""Async downloader for CFTC Disaggregated Futures Only ZIP archives."""

from __future__ import annotations

from types import TracebackType

import httpx

from open_cot.constants import BASE_URL

_DEFAULT_TIMEOUT = 60.0
_USER_AGENT = "open-cot/0.1.0 (https://github.com/spqr-quant/open-cot)"


class AsyncDownloader:
    """Downloads CFTC Disaggregated Futures Only ZIP files over HTTP.

    Intended to be used as an async context manager so the underlying
    ``httpx.AsyncClient`` is properly closed after use.

    Example::

        async with AsyncDownloader() as dl:
            data = await dl.download(2024)
    """

    def __init__(
        self,
        *,
        timeout: float = _DEFAULT_TIMEOUT,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._timeout = timeout
        self._external_client = client is not None
        self._client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(timeout),
            follow_redirects=True,
            headers={"User-Agent": _USER_AGENT},
        )

    async def __aenter__(self) -> AsyncDownloader:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if not self._external_client:
            await self._client.aclose()

    async def download(self, year: int) -> bytes:
        """Fetch the Disaggregated Futures Only ZIP for *year*.

        Args:
            year: Report year (e.g. 2024).

        Returns:
            Raw ZIP bytes.

        Raises:
            httpx.HTTPStatusError: If the CFTC server returns a non-2xx status.
            ValueError: If *year* is not a plausible report year.
        """
        if year < 2006 or year > 2100:
            raise ValueError(f"Year must be between 2006 and 2100, got {year}")

        url = BASE_URL.format(year=year)
        response = await self._client.get(url)
        response.raise_for_status()
        return response.content
