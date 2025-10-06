from itertools import chain
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    TypeVar,
    Union,
)
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

import dlt
from dlt.sources.helpers import requests
from requests import Session
import requests as requests_lib  # Import standard requests for exceptions

from .custom_fields_munger import rename_fields
from ..typing import TDataPage

# Configure logging for this module
logger = logging.getLogger(__name__)

# --- Concurrent and Robust Paginated Fetcher ---

def _fetch_page(
    session: Session,
    url: str,
    params: Dict[str, Any],
    max_retries: int = 3,
) -> Dict[str, Any]:
    """
    Fetches a single page of data with session-based requests,
    implementing retries with exponential backoff for rate limiting.
    """
    retries = 0
    while retries < max_retries:
        try:
            response = session.get(url, params=params, timeout=30)

            # If rate limited, wait and retry
            if response.status_code == 429:
                wait_time = int(response.headers.get("Retry-After", 60))
                logger.warning(
                    f"Rate limit hit for {url} with params {params}. "
                    f"Waiting {wait_time} seconds before retry {retries + 1}/{max_retries}."
                )
                time.sleep(wait_time)
                retries += 1
                continue

            response.raise_for_status()
            return response.json()

        except requests_lib.exceptions.HTTPError as e:
            # Handle 429 specifically
            if e.response.status_code == 429:
                wait_time = int(e.response.headers.get("Retry-After", 60))
                logger.warning(
                    f"Rate limit hit (caught in exception) for {url} with params {params}. "
                    f"Waiting {wait_time} seconds before retry {retries + 1}/{max_retries}."
                )
                time.sleep(wait_time)
                retries += 1
                if retries >= max_retries:
                    raise
                continue
            else:
                # For other HTTP errors, raise immediately
                raise

        except requests_lib.exceptions.RequestException as e:
            logger.error(f"Request failed for {url} with params {params}: {e}")
            retries += 1
            if retries >= max_retries:
                raise
            time.sleep(2 ** retries) # Exponential backoff for other errors

    raise Exception(f"Failed to fetch data for {url} after {max_retries} retries.")


def _paginated_get(
    url: str, headers: Dict[str, Any], params: Dict[str, Any]
) -> Iterator[List[Dict[str, Any]]]:
    """
    Fetches data from a paginated API endpoint concurrently.

    It assumes a 'start' and 'limit' style of pagination and fetches pages
    in parallel using a thread pool. It stops when an empty page is found
    or the API indicates no more items are available.

    Args:
        url: The base URL of the API endpoint.
        headers: Headers to be sent with each request.
        params: Initial parameters for the request (limit will be overwritten).

    Yields:
        A list of dictionaries for each page of data.
    """
    params["limit"] = 500
    start = 0

    # Use a session for connection pooling
    with requests.Session() as session:
        session.headers.update(headers)

        # Use a thread pool to fetch pages in parallel
        # Concurrency of 2 to stay within typical Pipedrive rate limits
        with ThreadPoolExecutor(max_workers=2) as executor:
            more_items = True
            while more_items:
                futures = []
                for i in range(executor._max_workers):
                    # Add small delay between submitting concurrent requests
                    if i > 0:
                        time.sleep(0.5)
                    futures.append(
                        executor.submit(_fetch_page, session, url, {**params, "start": start + (i * params["limit"])})
                    )
                futures = set(futures)
                
                pages_in_batch = 0
                for future in as_completed(futures):
                    page = future.result()
                    data = page.get("data")
                    pages_in_batch += 1
                    
                    if not data:
                        # This or a subsequent page is empty, stop fetching
                        more_items = False
                        # We should cancel remaining futures for this batch as they are likely empty too
                        for f in futures:
                            if not f.done():
                                f.cancel()
                        break
                    
                    yield data

                    pagination_info = page.get("additional_data", {}).get("pagination", {})
                    if not pagination_info.get("more_items_in_collection", False):
                        # API explicitly tells us there are no more pages
                        more_items = False
                        break # Exit the inner loop
                
                if more_items:
                    start += pages_in_batch * params["limit"]


def get_pages(
    entity: str, pipedrive_api_key: str, extra_params: Dict[str, Any] = None
) -> Iterator[List[Dict[str, Any]]]:
    """
    Generic method to retrieve endpoint data based on the required headers and params.

    Args:
        entity: the endpoint you want to call
        pipedrive_api_key:
        extra_params: any needed request params except pagination.

    Returns:

    """
    headers = {"Content-Type": "application/json"}
    params = {"api_token": pipedrive_api_key}
    if extra_params:
        params.update(extra_params)
    url = f"https://app.pipedrive.com/v1/{entity}"
    # The original headers are not used by our session-based fetcher, but we keep the arg for compatibility
    yield from _paginated_get(url, headers=headers, params=params)


def get_recent_items_incremental(
    entity: str,
    pipedrive_api_key: str,
    since_timestamp: dlt.sources.incremental[str] = dlt.sources.incremental(
        "update_time|modified", "1970-01-01 00:00:00"
    ),
) -> Iterator[TDataPage]:
    """Get a specific entity type from /recents with incremental state."""
    yield from _get_recent_pages(entity, pipedrive_api_key, since_timestamp.last_value)


T = TypeVar("T")


def _extract_recents_data(data: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Results from recents endpoint contain `data` key which is either a single entity or list of entities

    This returns a flat list of entities from an iterable of recent results
    """
    return [
        data_item
        for data_item in chain.from_iterable(
            (_list_wrapped(item["data"]) for item in data)
        )
        if data_item is not None
    ]


def _list_wrapped(item: Union[List[T], T]) -> List[T]:
    if isinstance(item, list):
        return item
    return [item]


def _get_recent_pages(
    entity: str, pipedrive_api_key: str, since_timestamp: str
) -> Iterator[TDataPage]:
    custom_fields_mapping = (
        dlt.current.source_state().get("custom_fields_mapping", {}).get(entity, {})
    )
    pages = get_pages(
        "recents",
        pipedrive_api_key,
        extra_params=dict(since_timestamp=since_timestamp, items=entity),
    )
    pages = (_extract_recents_data(page) for page in pages)
    for page in pages:
        yield rename_fields(page, custom_fields_mapping)


__source_name__ = "pipedrive"
