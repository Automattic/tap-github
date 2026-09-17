import time
import requests
from requests.models import PreparedRequest
import backoff
from simplejson import JSONDecodeError
import singer
from singer import metrics
from math import ceil

LOGGER = singer.get_logger()
DEFAULT_SLEEP_SECONDS = 600
DEFAULT_MIN_REMAIN_RATE_LIMIT = 0
DEFAULT_MAX_PER_PAGE = 100
DEFAULT_DOMAIN = "https://api.github.com"

# Set default timeout of 300 seconds
REQUEST_TIMEOUT = 300

# Length of GitHub's primary rate limit window.
RATE_LIMIT_WINDOW_SECONDS = 3600
# How many total seconds to retry when getting rate limit error from API. The quota is shared by
# every consumer of the GitHub account, so a fresh window can be drained again before we get to
# it: allow waiting through a few windows before giving up.
RATE_LIMIT_RETRY_MAX_TIME = 3 * RATE_LIMIT_WINDOW_SECONDS

PAGINATION_EXCEED_MSG = 'In order to keep the API fast for everyone, pagination is limited for this resource.'
# Matches both the primary ("API rate limit exceeded for ...") and the secondary
# ("You have exceeded a secondary rate limit ...") rejection messages.
RATE_LIMIT_MSG_FRAGMENT = 'rate limit'
# Safety margin added on top of `X-RateLimit-Reset` so we don't wake up a few seconds early.
RATE_LIMIT_RESET_MARGIN_SECONDS = 15
# GitHub's guidance when no header says how long to wait: "wait for at least one minute before retrying".
RATE_LIMIT_FALLBACK_WAIT_SECONDS = 60
# Every rate limit related header GitHub sends. The X-RateLimit-* values are per GitHub account
# (shared by all of its tokens), `Retry-After` only comes with secondary rate limit rejections.
RATE_LIMIT_HEADERS = ('X-RateLimit-Limit', 'X-RateLimit-Remaining', 'X-RateLimit-Used',
                      'X-RateLimit-Reset', 'X-RateLimit-Resource', 'Retry-After')

class GithubException(Exception):
    pass

class Server5xxError(GithubException):
    pass

class BadCredentialsException(GithubException):
    pass

class AuthException(GithubException):
    pass

class NotFoundException(GithubException):
    pass

class BadRequestException(GithubException):
    pass

class InternalServerError(Server5xxError):
    pass

class UnprocessableError(GithubException):
    pass

class NotModifiedError(GithubException):
    pass

class MovedPermanentlyError(GithubException):
    pass

class ConflictError(GithubException):
    pass

# Thrown when we receive 403 Rate Limit Exceeded from Github API
class RateLimitExceeded(GithubException):
    pass

# Thrown when we're expected to sleep for longer than the max_sleep_seconds limit
class RateLimitSleepExceeded(GithubException):
    pass

# Thrown when 429 is received from Github API
class TooManyRequests(GithubException):
    pass

# Thrown when repository is archived and extract_archived is not enabled
class ArchivedRepositoryError(GithubException):
    pass


ERROR_CODE_EXCEPTION_MAPPING = {
    301: {
        "raise_exception": MovedPermanentlyError,
        "message": "The resource you are looking for is moved to another URL."
    },
    304: {
        "raise_exception": NotModifiedError,
        "message": "The requested resource has not been modified since the last time you accessed it."
    },
    400:{
        "raise_exception": BadRequestException,
        "message": "The request is missing or has a bad parameter."
    },
    401: {
        "raise_exception": BadCredentialsException,
        "message": "Invalid authorization credentials."
    },
    403: {
        "raise_exception": AuthException,
        "message": "User doesn't have permission to access the resource."
    },
    404: {
        "raise_exception": NotFoundException,
        "message": "The resource you have specified cannot be found. Alternatively the access_token is not valid for the resource"
    },
    409: {
        "raise_exception": ConflictError,
        "message": "The request could not be completed due to a conflict with the current state of the server."
    },
    422: {
        "raise_exception": UnprocessableError,
        "message": "The request was not able to process right now."
    },
    429: {
        "raise_exception": TooManyRequests,
        "message": "Too many requests occurred."
    },
    500: {
        "raise_exception": InternalServerError,
        "message": "An error has occurred at Github's end."
    }
}

def raise_for_error(resp, source, stream, client, should_skip_404):
    """
    Retrieve the error code and the error message from the response and return custom exceptions accordingly.
    """
    error_code = resp.status_code
    try:
        response_json = resp.json()
    except JSONDecodeError:
        response_json = {}

    response_message = response_json.get('message', '')

    if is_rate_limit_rejection(resp, response_message):
        message = f"HTTP-error-code: {error_code}, Error: {response_message}"
        LOGGER.warning(message)
        # The quota is shared by every token of the GitHub account, so the request can be
        # rejected even though this tap stayed under `min_remain_rate_limit`: another consumer
        # drained the account in the meantime. Wait for the window to reset before retrying.
        wait_for_rate_limit_reset(resp)
        raise RateLimitExceeded(message) from None

    if error_code == 404 and should_skip_404:
        # Add not accessible stream into list.
        client.not_accessible_repos.add(stream)
        details = ERROR_CODE_EXCEPTION_MAPPING.get(error_code).get("message")
        if source == "teams":
            details += ' or it is a personal account repository'
        message = "HTTP-error-code: 404, Error: {}. Please refer \'{}\' for more details.".format(details, response_json.get("documentation_url"))
        LOGGER.warning(message)
        # Don't raise a NotFoundException
        return None

    if error_code == 422 and PAGINATION_EXCEED_MSG in response_message:
        message = f"HTTP-error-code: 422, Error: {response_message}. " \
                  f"Please refer '{response_json.get('documentation_url')}' for more details." \
                  "This is a known issue when the results exceed 40k and the last page is not full" \
                  " (it will trim the results to get only the available by the API)."
        LOGGER.warning(message)
        return None

    message = "HTTP-error-code: {}, Error: {}".format(
        error_code, ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get("message", "Unknown Error") if response_json == {} else response_json)

    if error_code > 500:
        raise Server5xxError(message) from None

    exc = ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get("raise_exception", GithubException)
    raise exc(message) from None

def calculate_seconds(epoch):
    """
    Calculate the seconds to sleep before making a new request.
    """
    current = time.time()
    return max(0, int(ceil(epoch - current)))

def log_rate_limit_headers(response, url):
    """
    Debug-log the rate limit headers of every response, so a sync log shows how the account's
    quota (shared by every consumer of the GitHub account) evolves request by request.
    """
    values = {name: response.headers[name] for name in RATE_LIMIT_HEADERS if name in response.headers}
    if not values:
        LOGGER.debug("Rate limit: no rate limit headers in response (HTTP %s) for %s", response.status_code, url)
        return
    try:
        values['seconds_until_reset'] = calculate_seconds(int(values['X-RateLimit-Reset']))
    except (KeyError, ValueError):
        pass
    LOGGER.debug("Rate limit: %s (HTTP %s) for %s",
                 ', '.join('{}={}'.format(name, value) for name, value in values.items()), response.status_code, url)

def seconds_until_rate_limit_reset(response):
    """
    Seconds to wait for the primary rate limit window to reset, based on `X-RateLimit-Reset`.
    Returns None when the header is absent.
    """
    if 'X-RateLimit-Reset' not in response.headers:
        return None
    return calculate_seconds(int(response.headers['X-RateLimit-Reset']) + RATE_LIMIT_RESET_MARGIN_SECONDS)

def is_rate_limit_rejection(response, response_message=''):
    """
    Whether a non-200 response is GitHub rejecting the request because of a rate limit
    (primary or secondary). GitHub answers with 403 or 429 in both cases.

    The primary quota is per GitHub *account*, shared by every token / app of that account,
    so a rejection can happen even when this tap never went under `min_remain_rate_limit`.
    """
    if response.status_code not in (403, 429):
        return False
    if 'Retry-After' in response.headers:
        return True
    if str(response.headers.get('X-RateLimit-Remaining', '')) == '0':
        return True
    return RATE_LIMIT_MSG_FRAGMENT in response_message.lower()

def wait_for_rate_limit_reset(response):
    """
    Sleep until a *rejected* request can be retried: `Retry-After` for secondary limits,
    `X-RateLimit-Reset` for the primary limit. The quota is already gone (possibly consumed by
    another user of the same GitHub account), so `max_sleep_seconds` is not applied here: the
    only alternative to waiting is failing the sync. The wait is capped at the length of a
    rate-limit window; when no header is present nothing is slept here and the caller's backoff
    applies GitHub's fallback guidance (retry after at least one minute).
    """
    if 'Retry-After' in response.headers:
        seconds_to_sleep = int(response.headers['Retry-After'])
        reason = "Secondary rate limit hit (Retry-After header)"
    else:
        seconds_to_sleep = seconds_until_rate_limit_reset(response)
        reason = "Account rate limit exhausted (X-RateLimit-Remaining: {}, X-RateLimit-Limit: {})".format(
            response.headers.get('X-RateLimit-Remaining'), response.headers.get('X-RateLimit-Limit'))

    if not seconds_to_sleep:
        return
    seconds_to_sleep = min(seconds_to_sleep, RATE_LIMIT_WINDOW_SECONDS)
    LOGGER.info("%s. Tap will retry the data collection after %s seconds.", reason, seconds_to_sleep)
    time.sleep(seconds_to_sleep)

def rate_throttling(response, max_sleep_seconds, min_remain_rate_limit, base_url=DEFAULT_DOMAIN):
    """
    Throttle after a successful request so the tap leaves at least `min_remain_rate_limit`
    requests of the account's hourly quota untouched.

    `X-RateLimit-Remaining` reports the quota left for the whole GitHub account (every token,
    OAuth app or GitHub App acting as that user shares it), not only for this tap, so the
    reserve is what other consumers of the account get to keep.
    """
    if "Retry-After" in response.headers:
        # handles the secondary rate limit
        seconds_to_sleep = int(response.headers['Retry-After'])
        if seconds_to_sleep > max_sleep_seconds:
            message = "Secondary rate limit exceeded, please try after {} seconds.".format(seconds_to_sleep)
            raise RateLimitSleepExceeded(message) from None
        LOGGER.info("Retry-After header found in response. Tap will retry the data collection after %s seconds.", seconds_to_sleep)
        time.sleep(seconds_to_sleep)
    if 'X-RateLimit-Remaining' in response.headers:
        remaining = int(response.headers['X-RateLimit-Remaining'])
        if remaining <= min_remain_rate_limit:
            seconds_to_sleep = seconds_until_rate_limit_reset(response)
            if seconds_to_sleep is None:
                # Should not happen on github.com (the header is always sent) but follow GitHub's
                # fallback guidance rather than keep consuming the shared quota.
                LOGGER.warning("X-RateLimit-Remaining is %s but X-RateLimit-Reset header is missing, waiting %s seconds.",
                               remaining, RATE_LIMIT_FALLBACK_WAIT_SECONDS)
                seconds_to_sleep = RATE_LIMIT_FALLBACK_WAIT_SECONDS

            if seconds_to_sleep > max_sleep_seconds:
                message = "API rate limit exceeded, please try after {} seconds.".format(seconds_to_sleep)
                raise RateLimitSleepExceeded(message) from None

            LOGGER.info("Account rate limit reserve reached (X-RateLimit-Remaining: %s, X-RateLimit-Limit: %s, "
                        "min_remain_rate_limit: %s). The quota is shared by every token of this GitHub account. "
                        "Tap will retry the data collection after %s seconds.",
                        remaining, response.headers.get('X-RateLimit-Limit'), min_remain_rate_limit, seconds_to_sleep)
            time.sleep(seconds_to_sleep)
    elif base_url == DEFAULT_DOMAIN:
        # On github.com a missing `X-RateLimit-Remaining` header means the base URL is not a
        # valid GitHub domain. GitHub Enterprise (a custom base_url) can omit the header even on
        # a successful response, so its absence there is not an error -- just skip throttling.
        raise GithubException("The API call using the specified base url was unsuccessful. Please double-check the provided base URL.")

class GithubClient:
    """
    The client class used for making REST calls to the Github API.
    """
    def __init__(self, config):
        self.config = config
        self.session = requests.Session()
        self.base_url = config['base_url'] if config.get('base_url') else DEFAULT_DOMAIN
        self.max_sleep_seconds = self.config.get('max_sleep_seconds', DEFAULT_SLEEP_SECONDS)
        self.min_remain_rate_limit = self.config.get('min_remain_rate_limit', DEFAULT_MIN_REMAIN_RATE_LIMIT)
        self.set_auth_in_session()
        self.not_accessible_repos = set()
        self.max_per_page = self.config.get('max_per_page', DEFAULT_MAX_PER_PAGE)
        # Convert string 'true'/'false' to boolean, default to False
        extract_archived_value = str(self.config.get('extract_archived', 'false')).lower()
        self.extract_archived = extract_archived_value == 'true'

    def get_request_timeout(self):
        """
        Get the request timeout from the config, if not present use the default 300 seconds.
        """
        # Get the value of request timeout from config
        config_request_timeout = self.config.get('request_timeout')

        # Only return the timeout value if it is passed in the config and the value is not 0, "0" or ""
        if config_request_timeout and float(config_request_timeout):
            return float(config_request_timeout)

        # Return default timeout
        return REQUEST_TIMEOUT

    def set_auth_in_session(self):
        """
        Set access token in the header for authorization.
        """
        access_token = self.config['access_token']
        self.session.headers.update({'authorization': 'token ' + access_token})

    # pylint: disable=dangerous-default-value
    # During 'Timeout' error there is also possibility of 'ConnectionError',
    # hence added backoff for 'ConnectionError' too.
    @backoff.on_exception(backoff.expo, (requests.Timeout, requests.ConnectionError, Server5xxError, TooManyRequests),
                          max_tries=5, factor=2)
    @backoff.on_exception(backoff.expo, (BadCredentialsException, ), max_tries=3, factor=2)
    @backoff.on_exception(backoff.constant, (RateLimitExceeded, ), interval=60, jitter=None, max_time=RATE_LIMIT_RETRY_MAX_TIME)
    def authed_get_single_page(self, source, url, headers={}, stream="", should_skip_404 = True):
        """
        Call rest API and return the response in case of status code 200.
        """
        with metrics.http_request_timer(url) as timer:
            self.session.headers.update(headers)
            resp = self.session.request(method='get', url=url, timeout=self.get_request_timeout())
            log_rate_limit_headers(resp, url)
            if resp.status_code != 200:
                raise_for_error(resp, source, stream, self, should_skip_404)
            timer.tags[metrics.Tag.http_status_code] = resp.status_code
            rate_throttling(resp, self.max_sleep_seconds, self.min_remain_rate_limit, self.base_url)
            if resp.status_code == 404 or resp.status_code == 422:
                # Return an empty response body since we're not raising a NotFoundException
                resp._content = b'{}'  # pylint: disable=protected-access
            return resp

    def authed_get_all_pages(self, source, url, headers={}, stream="", should_skip_404 = True):
        """
        Fetch all pages of records and return them.
        """
        next_url = self.prepare_url(url)
        while next_url:
            response = self.authed_get_single_page(source, next_url, headers, stream, should_skip_404)
            yield response

            next_url = response.links.get('next', {}).get('url', None)

    def authed_get(self, source, url, headers={}, stream="", should_skip_404=True, single_page=False):
        if single_page:
            yield self.authed_get_single_page(source, url, headers, stream, should_skip_404)
        else:
            yield from self.authed_get_all_pages(source, url, headers, stream, should_skip_404)

    def prepare_url(self, url):
        """
        Prepare the URL with some additional parameters
        """
        prepared_request = PreparedRequest()
        # Including max per page param
        prepared_request.prepare_url(url, {'per_page': self.max_per_page})
        return prepared_request.url

    def verify_repo_access(self, url_for_repo, repo):
        """
        Call rest API to verify that the user has sufficient permissions to access this repository.
        """
        try:
            self.authed_get_single_page("verifying repository access", url_for_repo, should_skip_404=False)
        except NotFoundException:
            # Throwing user-friendly error message as it checks token access
            message = "HTTP-error-code: 404, Error: Please check the repository name \'{}\' or you do not have sufficient permissions to access this repository.".format(repo)
            raise NotFoundException(message) from None

    def check_repo_archived(self, repo):
        """
        Check if a repository is archived and raise an error if extract_archived is not enabled.

        Args:
            repo: Repository in 'org/repo' format

        Raises:
            ArchivedRepositoryError: If repo is archived and extract_archived config is not true
        """
        url = "{}/repos/{}".format(self.base_url, repo)
        response = self.authed_get_single_page("checking repository archived status", url, should_skip_404=False)
        repo_info = response.json()

        if repo_info.get('archived', False):
            if not self.extract_archived:
                message = "Repository '{}' is archived. To extract data from archived repositories, " \
                          "set 'extract_archived' to 'true' in the config.".format(repo)
                raise ArchivedRepositoryError(message)
            LOGGER.warning("Repository '%s' is archived. Proceeding with extraction as 'extract_archived' is enabled.", repo)

    def verify_access_for_repo(self):
        """
        For all the repositories mentioned in the config, check the access for each repos.
        Also checks if repositories are archived and fails if extract_archived is not enabled.
        """
        repositories, org = self.extract_repos_from_config() # pylint: disable=unused-variable

        for repo in repositories:

            url_for_repo = "{}/repos/{}/commits".format(self.base_url, repo)
            LOGGER.info("Verifying access of repository: %s", repo)

            # Verifying for Repo access
            self.verify_repo_access(url_for_repo, repo)

            # Check if repository is archived
            self.check_repo_archived(repo)

    def extract_orgs_from_config(self):
        """
        Extracts all organizations from the config
        """
        repo_paths = list(filter(None, self.config['repository'].split(' ')))
        orgs_paths = [repo.split('/')[0] for repo in repo_paths]

        return set(orgs_paths)

    def extract_repos_from_config(self):
        """
        Extracts all repositories from the config and calls get_all_repos()
        for organizations using the wildcard 'org/*' format.
        """
        repo_paths = list(filter(None, self.config['repository'].split(' ')))

        unique_repos = set()
        # Insert the duplicate repos found in the config repo_paths into duplicates
        duplicate_repos = [x for x in repo_paths if x in unique_repos or (unique_repos.add(x) or False)]
        if duplicate_repos:
            LOGGER.warning("Duplicate repositories found: %s and will be synced only once.", duplicate_repos)

        repo_paths = list(set(repo_paths))

        orgs_with_all_repos = []
        orgs = []
        repos_with_errors = []
        for repo in repo_paths:
            # Split the repo_path by `/` as we are passing org/repo_name in the config.
            split_repo_path = repo.split('/')
            # Prepare list of organizations
            orgs.append(split_repo_path[0])
            # Check for the second element in the split list only if the length is greater than 1 and the first/second
            # element is not empty (for scenarios such as: `org/` or `/repo` which is invalid)
            if len(split_repo_path) > 1 and split_repo_path[1] != '' and split_repo_path[0] != '':
                # If the second element is *, we need to check access for all the repos.
                if split_repo_path[1] == '*':
                    orgs_with_all_repos.append(repo)
            else:
                # If `/`/repo name/organization not found, append the repo_path in the repos_with_errors
                repos_with_errors.append(repo)

        # If any repos found in repos_with_errors, raise an exception
        if repos_with_errors:
            raise GithubException("Please provide valid organization/repository for: {}".format(sorted(repos_with_errors)))

        if orgs_with_all_repos:
            # Remove any wildcard "org/*" occurrences from `repo_paths`
            repo_paths = list(set(repo_paths).difference(set(orgs_with_all_repos)))

            # Get all repositories for an org in the config
            all_repos = self.get_all_repos(orgs_with_all_repos)

            # Update repo_paths
            repo_paths.extend(all_repos)

        return repo_paths, set(orgs)

    def get_all_repos(self, organizations: list):
        """
        Retrieves all repositories for the provided organizations and
        verifies basic access for them.

        Docs: https://docs.github.com/en/rest/reference/repos#list-organization-repositories
        """
        repos = []

        for org_path in organizations:
            org = org_path.split('/')[0]
            try:
                for response in self.authed_get_all_pages(
                    'get_all_repos',
                    '{}/orgs/{}/repos?sort=created&direction=desc'.format(self.base_url, org),
                    should_skip_404 = False
                ):
                    org_repos = response.json()
                    LOGGER.info("Collected repos for organization: %s", org)

                    for repo in org_repos:
                        repo_full_name = repo.get('full_name')
                        LOGGER.info("Verifying access of repository: %s", repo_full_name)

                        self.verify_repo_access(
                            '{}/repos/{}/commits'.format(self.base_url,repo_full_name),
                            repo
                        )

                        # Check if repository is archived (info already available in response)
                        if repo.get('archived', False):
                            if not self.extract_archived:
                                message = "Repository '{}' is archived. To extract data from archived repositories, " \
                                          "set 'extract_archived' to 'true' in the config.".format(repo_full_name)
                                raise ArchivedRepositoryError(message)
                            LOGGER.warning("Repository '%s' is archived. Proceeding with extraction as 'extract_archived' is enabled.", repo_full_name)

                        repos.append(repo_full_name)
            except NotFoundException:
                # Throwing user-friendly error message as it checks token access
                message = "HTTP-error-code: 404, Error: Please check the organization name \'{}\' or you do not have sufficient permissions to access this organization.".format(org)
                raise NotFoundException(message) from None

        return repos

    def __exit__(self, exception_type, exception_value, traceback):
        # Kill the session instance.
        self.session.close()
