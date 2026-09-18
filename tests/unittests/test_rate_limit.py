import tap_github
from tap_github.client import (
    GithubClient, rate_throttling, is_rate_limit_rejection, wait_for_rate_limit_reset,
    GithubException, RateLimitExceeded, RateLimitSleepExceeded, AuthException, TooManyRequests,
    RATE_LIMIT_RESET_MARGIN_SECONDS, RATE_LIMIT_WINDOW_SECONDS, RATE_LIMIT_FALLBACK_WAIT_SECONDS,
)
import json
import unittest
from unittest import mock
from math import ceil
import time
import requests

DEFAULT_SLEEP_SECONDS = 600
DEFAULT_MIN_REMAIN_RATE_LIMIT = 0


def make_response(status_code=200, headers=None, json_body=None):
    """Build a `requests.Response` without touching the network."""
    resp = requests.Response()
    resp.status_code = status_code
    resp.headers = requests.structures.CaseInsensitiveDict(headers or {})
    resp._content = b'{}' if json_body is None else json.dumps(json_body).encode()  # pylint: disable=protected-access
    return resp


def reset_in(seconds):
    return int(ceil(time.time())) + seconds


@mock.patch('time.sleep')
class TestRateThrottling(unittest.TestCase):
    """
    Test `rate_throttling` (throttle applied after a successful request).
    """

    def test_rate_limt_wait(self, mocked_sleep):
        """
        Sleep until reset (plus safety margin) when the remaining quota hits the reserve
        and the wait is below `max_sleep_seconds`.
        """
        resp = make_response(headers={"X-RateLimit-Reset": reset_in(120), "X-RateLimit-Remaining": 0})

        rate_throttling(resp, DEFAULT_SLEEP_SECONDS, DEFAULT_MIN_REMAIN_RATE_LIMIT)

        mocked_sleep.assert_called_with(120 + RATE_LIMIT_RESET_MARGIN_SECONDS + 1)

    def test_rate_limit_exception(self, mocked_sleep):
        """
        Raise `RateLimitSleepExceeded` when the wait is longer than `max_sleep_seconds`.
        """
        resp = make_response(headers={"X-RateLimit-Reset": reset_in(601), "X-RateLimit-Remaining": 0})

        with self.assertRaises(RateLimitSleepExceeded) as e:
            rate_throttling(resp, DEFAULT_SLEEP_SECONDS, DEFAULT_MIN_REMAIN_RATE_LIMIT)
        self.assertEqual(str(e.exception), "API rate limit exceeded, please try after {} seconds.".format(601 + RATE_LIMIT_RESET_MARGIN_SECONDS + 1))
        self.assertFalse(mocked_sleep.called)

    def test_rate_limit_not_exceeded(self, mocked_sleep):
        """
        No sleep when the remaining quota is above the reserve.
        """
        resp = make_response(headers={"X-RateLimit-Reset": reset_in(10), "X-RateLimit-Remaining": 5})

        rate_throttling(resp, DEFAULT_SLEEP_SECONDS, DEFAULT_MIN_REMAIN_RATE_LIMIT)

        self.assertFalse(mocked_sleep.called)

    def test_rate_limit_wait_with_min_remain_rate_limit_defined(self, mocked_sleep):
        """
        Sleep when the remaining quota is equal to `min_remain_rate_limit`: the reserve is left
        to the other consumers of the same GitHub account.
        """
        resp = make_response(headers={"X-RateLimit-Reset": reset_in(10), "X-RateLimit-Remaining": 5})

        rate_throttling(resp, DEFAULT_SLEEP_SECONDS, min_remain_rate_limit=5)

        self.assertTrue(mocked_sleep.called)

    def test_rate_limit_remaining_drained_by_another_consumer(self, mocked_sleep):
        """
        The header reflects the whole account: when another token of the account used the quota
        the tap must throttle even though it barely made any requests itself.
        """
        resp = make_response(headers={"X-RateLimit-Limit": 5000, "X-RateLimit-Used": 4600,
                                      "X-RateLimit-Reset": reset_in(10), "X-RateLimit-Remaining": 400})

        rate_throttling(resp, DEFAULT_SLEEP_SECONDS, min_remain_rate_limit=1000)

        self.assertTrue(mocked_sleep.called)

    def test_retry_after_on_success_bounded_by_max_sleep(self, mocked_sleep):
        """
        `Retry-After` is honoured but never beyond `max_sleep_seconds`.
        """
        resp = make_response(headers={"Retry-After": 30, "X-RateLimit-Reset": reset_in(10), "X-RateLimit-Remaining": 5})
        rate_throttling(resp, DEFAULT_SLEEP_SECONDS, DEFAULT_MIN_REMAIN_RATE_LIMIT)
        mocked_sleep.assert_called_with(30)

        resp = make_response(headers={"Retry-After": 601, "X-RateLimit-Reset": reset_in(10), "X-RateLimit-Remaining": 5})
        with self.assertRaises(RateLimitSleepExceeded):
            rate_throttling(resp, DEFAULT_SLEEP_SECONDS, DEFAULT_MIN_REMAIN_RATE_LIMIT)

    def test_missing_reset_header_waits_fallback(self, mocked_sleep):
        """
        A response with `X-RateLimit-Remaining` at the reserve but no `X-RateLimit-Reset` must
        not crash nor keep going: follow GitHub's fallback guidance and wait one minute.
        """
        resp = make_response(headers={"X-RateLimit-Remaining": 0})

        rate_throttling(resp, DEFAULT_SLEEP_SECONDS, DEFAULT_MIN_REMAIN_RATE_LIMIT)

        mocked_sleep.assert_called_with(RATE_LIMIT_FALLBACK_WAIT_SECONDS)

    def test_rate_limt_header_not_found(self, mocked_sleep):
        """
        Raise if `X-RateLimit-Remaining` is missing on github.com (invalid base URL).
        """
        resp = make_response(headers={})

        with self.assertRaises(GithubException) as e:
            rate_throttling(resp, DEFAULT_SLEEP_SECONDS, DEFAULT_MIN_REMAIN_RATE_LIMIT)

        self.assertEqual(str(e.exception), "The API call using the specified base url was unsuccessful. Please double-check the provided base URL.")

    def test_rate_limit_header_not_found_custom_base_url(self, mocked_sleep):
        """
        Do not raise when `X-RateLimit-Remaining` is missing for a custom base URL:
        GitHub Enterprise can omit the header even on a successful response.
        """
        resp = make_response(headers={})

        rate_throttling(resp, DEFAULT_SLEEP_SECONDS, DEFAULT_MIN_REMAIN_RATE_LIMIT, base_url="https://github.example.com/api/v3")
        self.assertFalse(mocked_sleep.called)


class TestLogRateLimitHeaders(unittest.TestCase):
    """
    Test the per-response debug log of the rate limit headers.
    """

    @mock.patch("time.time", return_value=1_000_000.0)
    @mock.patch("tap_github.client.LOGGER.debug")
    def test_logs_all_headers_and_seconds_until_reset(self, mocked_debug, _mocked_time):
        resp = make_response(200, headers={"X-RateLimit-Limit": 5000, "X-RateLimit-Remaining": 291, "X-RateLimit-Used": 4709,
                                           "X-RateLimit-Reset": 1_000_120, "X-RateLimit-Resource": "core"})

        tap_github.client.log_rate_limit_headers(resp, "https://api.github.com/repos/org/repo/commits")

        message = mocked_debug.call_args[0][0] % mocked_debug.call_args[0][1:]
        self.assertIn("X-RateLimit-Limit=5000", message)
        self.assertIn("X-RateLimit-Remaining=291", message)
        self.assertIn("X-RateLimit-Used=4709", message)
        self.assertIn("X-RateLimit-Resource=core", message)
        self.assertIn("seconds_until_reset=120 (HTTP 200)", message)
        self.assertIn("(HTTP 200) for https://api.github.com/repos/org/repo/commits", message)
        self.assertNotIn("Retry-After", message)

    @mock.patch("tap_github.client.LOGGER.debug")
    def test_logs_retry_after_on_rejection(self, mocked_debug):
        resp = make_response(429, headers={"Retry-After": 60, "X-RateLimit-Remaining": 4000})

        tap_github.client.log_rate_limit_headers(resp, "url")

        message = mocked_debug.call_args[0][0] % mocked_debug.call_args[0][1:]
        self.assertIn("Retry-After=60", message)
        self.assertIn("(HTTP 429)", message)

    @mock.patch("tap_github.client.LOGGER.debug")
    def test_logs_absence_of_headers(self, mocked_debug):
        resp = make_response(200, headers={})

        tap_github.client.log_rate_limit_headers(resp, "url")

        message = mocked_debug.call_args[0][0] % mocked_debug.call_args[0][1:]
        self.assertIn("no rate limit headers", message)


class TestIsRateLimitRejection(unittest.TestCase):
    """
    Test detection of 403/429 responses that are rate limit rejections.
    """

    def test_primary_limit_by_header(self):
        resp = make_response(403, headers={"X-RateLimit-Remaining": 0, "X-RateLimit-Reset": reset_in(10)})
        self.assertTrue(is_rate_limit_rejection(resp, ""))

    def test_primary_limit_by_message(self):
        resp = make_response(403, headers={})
        self.assertTrue(is_rate_limit_rejection(resp, "API rate limit exceeded for user ID 12345."))

    def test_secondary_limit_by_message(self):
        resp = make_response(403, headers={"X-RateLimit-Remaining": 4000})
        self.assertTrue(is_rate_limit_rejection(resp, "You have exceeded a secondary rate limit. Please wait a few minutes before you try again."))

    def test_secondary_limit_by_retry_after(self):
        resp = make_response(429, headers={"Retry-After": 60})
        self.assertTrue(is_rate_limit_rejection(resp, ""))

    def test_plain_403_is_not_rate_limit(self):
        resp = make_response(403, headers={"X-RateLimit-Remaining": 1})
        self.assertFalse(is_rate_limit_rejection(resp, "Resource not accessible by personal access token"))

    def test_other_status_is_not_rate_limit(self):
        resp = make_response(500, headers={"X-RateLimit-Remaining": 0})
        self.assertFalse(is_rate_limit_rejection(resp, "API rate limit exceeded"))


@mock.patch('time.sleep')
class TestWaitForRateLimitReset(unittest.TestCase):
    """
    Test the wait applied after a rejected request.
    """

    MAX_SLEEP = 4000

    def test_primary_exhausted_waits_until_reset(self, mocked_sleep):
        resp = make_response(403, headers={"X-RateLimit-Remaining": 0, "X-RateLimit-Reset": reset_in(2000)})
        wait_for_rate_limit_reset(resp, self.MAX_SLEEP)
        mocked_sleep.assert_called_with(2000 + RATE_LIMIT_RESET_MARGIN_SECONDS + 1)

    def test_prefers_retry_after(self, mocked_sleep):
        resp = make_response(429, headers={"Retry-After": 45, "X-RateLimit-Remaining": 0, "X-RateLimit-Reset": reset_in(2000)})
        wait_for_rate_limit_reset(resp, self.MAX_SLEEP)
        mocked_sleep.assert_called_with(45)

    def test_secondary_without_retry_after_waits_one_minute_not_primary_reset(self, mocked_sleep):
        """
        Secondary rate limit rejection: no `Retry-After`, quota still available. Must not sleep
        until the primary reset (50 minutes away here) but follow the one-minute fallback.
        """
        resp = make_response(403, headers={"X-RateLimit-Remaining": 4000, "X-RateLimit-Reset": reset_in(3000)})
        wait_for_rate_limit_reset(resp, self.MAX_SLEEP)
        mocked_sleep.assert_called_with(RATE_LIMIT_FALLBACK_WAIT_SECONDS)

    def test_wait_capped_to_one_window(self, mocked_sleep):
        resp = make_response(403, headers={"X-RateLimit-Remaining": 0, "X-RateLimit-Reset": reset_in(10 * 3600)})
        wait_for_rate_limit_reset(resp, self.MAX_SLEEP)
        mocked_sleep.assert_called_with(RATE_LIMIT_WINDOW_SECONDS)

    def test_no_headers_waits_one_minute(self, mocked_sleep):
        resp = make_response(403, headers={})
        wait_for_rate_limit_reset(resp, self.MAX_SLEEP)
        mocked_sleep.assert_called_with(RATE_LIMIT_FALLBACK_WAIT_SECONDS)

    def test_wait_above_max_sleep_seconds_raises(self, mocked_sleep):
        resp = make_response(403, headers={"X-RateLimit-Remaining": 0, "X-RateLimit-Reset": reset_in(2000)})
        with self.assertRaises(RateLimitSleepExceeded) as e:
            wait_for_rate_limit_reset(resp, max_sleep_seconds=600)
        self.assertIn("please try after {} seconds.".format(2000 + RATE_LIMIT_RESET_MARGIN_SECONDS + 1), str(e.exception))
        self.assertFalse(mocked_sleep.called)


@mock.patch("time.sleep")
@mock.patch("requests.Session.request")
class TestRateLimitRejectionInClient(unittest.TestCase):
    """
    Test `authed_get_single_page` when GitHub rejects the request because the account quota
    is exhausted (possibly by another consumer of the same account).
    """

    config = {"access_token": "", "repository": "singer-io/tap-github"}

    def _rejected(self, status_code, headers, message):
        return make_response(status_code, headers=headers, json_body={"message": message})

    def _ok(self):
        return make_response(200, headers={"X-RateLimit-Remaining": 4999, "X-RateLimit-Reset": reset_in(3000)})

    def test_403_primary_limit_waits_for_reset_then_retries(self, mocked_request, mocked_sleep):
        reset = reset_in(1200)
        mocked_request.side_effect = [
            self._rejected(403, {"X-RateLimit-Remaining": 0, "X-RateLimit-Reset": reset, "X-RateLimit-Limit": 5000},
                           "API rate limit exceeded for user ID 12345."),
            self._ok(),
        ]

        resp = GithubClient(self.config).authed_get_single_page("", "")

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(mocked_request.call_count, 2)
        # Slept until the window reset (+ margin), within the default max_sleep_seconds.
        self.assertIn(mock.call(1200 + RATE_LIMIT_RESET_MARGIN_SECONDS + 1), mocked_sleep.mock_calls)
        # The retry itself adds no extra delay on top of that wait.
        self.assertEqual([c for c in mocked_sleep.mock_calls if c != mock.call(0)],
                         [mock.call(1200 + RATE_LIMIT_RESET_MARGIN_SECONDS + 1)])

    def test_rejection_beyond_max_sleep_seconds_fails_without_retry(self, mocked_request, mocked_sleep):
        mocked_request.return_value = self._rejected(403, {"X-RateLimit-Remaining": 0, "X-RateLimit-Reset": reset_in(1200)},
                                                     "API rate limit exceeded for user ID 12345.")

        with self.assertRaises(RateLimitSleepExceeded):
            GithubClient({**self.config, "max_sleep_seconds": 600}).authed_get_single_page("", "")
        self.assertEqual(mocked_request.call_count, 1)
        self.assertFalse(mocked_sleep.called)

    def test_429_primary_limit_is_retried_as_rate_limit(self, mocked_request, mocked_sleep):
        mocked_request.side_effect = [
            self._rejected(429, {"X-RateLimit-Remaining": 0, "X-RateLimit-Reset": reset_in(300)},
                           "API rate limit exceeded for user ID 12345."),
            self._ok(),
        ]

        resp = GithubClient(self.config).authed_get_single_page("", "")

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(mocked_request.call_count, 2)

    def test_403_secondary_limit_is_retried_not_auth_error(self, mocked_request, mocked_sleep):
        mocked_request.side_effect = [
            self._rejected(403, {"Retry-After": 60, "X-RateLimit-Remaining": 4000},
                           "You have exceeded a secondary rate limit. Please wait a few minutes before you try again."),
            self._ok(),
        ]

        resp = GithubClient(self.config).authed_get_single_page("", "")

        self.assertEqual(resp.status_code, 200)
        self.assertIn(mock.call(60), mocked_sleep.mock_calls)

    def test_plain_403_still_raises_auth_exception(self, mocked_request, mocked_sleep):
        mocked_request.return_value = self._rejected(403, {"X-RateLimit-Remaining": 10}, "Resource not accessible by personal access token")

        with self.assertRaises(AuthException):
            GithubClient(self.config).authed_get_single_page("", "")
        self.assertEqual(mocked_request.call_count, 1)

    def test_429_without_rate_limit_signal_still_too_many_requests(self, mocked_request, mocked_sleep):
        mocked_request.return_value = self._rejected(429, {"X-RateLimit-Remaining": 10}, "")

        with self.assertRaises(TooManyRequests):
            GithubClient(self.config).authed_get_single_page("", "")
        self.assertEqual(mocked_request.call_count, 5)

    def test_rate_limit_exceeded_message(self, mocked_request, mocked_sleep):
        resp = self._rejected(403, {"X-RateLimit-Remaining": 0, "X-RateLimit-Reset": reset_in(10)}, "API rate limit exceeded")
        with self.assertRaises(RateLimitExceeded) as e:
            tap_github.client.raise_for_error(resp, "", "", None, True)
        self.assertEqual(str(e.exception), "HTTP-error-code: 403, Error: API rate limit exceeded")
