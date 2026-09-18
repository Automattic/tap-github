# tap-github

This is a [Singer](https://singer.io) tap that produces JSON-formatted
data from the GitHub API following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

This tap:
- Pulls raw data from the [GitHub REST API](https://developer.github.com/v3/)
- Extracts the following resources from GitHub for a single repository:
  - [Assignees](https://docs.github.com/en/rest/reference/issues#list-assigneess)
  - [Collaborators](https://docs.github.com/en/rest/reference/repos#list-repository-collaborators)
  - [Commits](https://docs.github.com/en/rest/reference/repos#list-commits)
  - [Commit Comments](https://docs.github.com/en/rest/reference/repos#list-commit-comments-for-a-repository)
  - [Events](https://docs.github.com/en/rest/reference/issues#events)
  - [Issues](https://docs.github.com/en/rest/reference/issues#list-repository-issues)
  - [Issue Events](https://docs.github.com/en/rest/reference/issues#list-issue-events-for-a-repository)
  - [Issue Milestones](https://docs.github.com/en/rest/reference/issues#list-milestones)
  - [Projects](https://docs.github.com/en/rest/reference/projects#list-repository-projects)
  - [Project Cards](https://docs.github.com/en/rest/reference/projects#list-project-cards)
  - [Project Columns](https://docs.github.com/en/rest/reference/projects#list-project-columns)
  - [Pull Requests](https://docs.github.com/en/rest/reference/pulls#list-pull-requests)
  - [PR Commits](https://docs.github.com/en/rest/reference/pulls#list-commits-on-a-pull-request)
  - [Releases](https://docs.github.com/en/rest/reference/repos#list-releases)
  - [Comments](https://docs.github.com/en/rest/reference/issues#list-issue-comments-for-a-repository)
  - [Reviews](https://docs.github.com/en/rest/reference/pulls#list-reviews-for-a-pull-request)
  - [Review Comments](https://docs.github.com/en/rest/reference/pulls#list-review-comments-in-a-repository)
  - [Stargazers](https://docs.github.com/en/rest/reference/activity#list-stargazers)
  - [Teams](https://docs.github.com/en/rest/reference/teams#list-teams)
  - [Team Members](https://docs.github.com/en/rest/reference/teams#list-team-members)
  - [Team Memberships](https://docs.github.com/en/rest/reference/teams#get-team-membership-for-a-user)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Quick start

1. Install

   We recommend using a virtualenv:

    ```bash
    > virtualenv -p python3 venv
    > source venv/bin/activate
    > pip install tap-github
    ```

2. Create a GitHub access token

    Login to your GitHub account, go to the
    [Personal Access Tokens](https://github.com/settings/tokens) settings
    page, and generate a new token with at least the `repo` scope. Save this
    access token, you'll need it for the next step.

3. Create the config file

    Create a JSON file containing the start date, access token you just created
    and the path to one or multiple repositories that you want to extract data from. Each repo path should be space delimited. The repo path is relative to `"base_url"`
    (Default: `https://github.com/`). For example the path for this repository is
    `singer-io/tap-github`. You can also add request timeout to set the timeout for requests which is an optional parameter with default value of 300 seconds.

    ```json
    {
      "access_token": "your-access-token",
      "repository": "singer-io/tap-github singer-io/getting-started",
      "start_date": "2021-01-01T00:00:00Z",
      "request_timeout": 300,
      "base_url": "https://api.github.com"
    }
    ```

> Note: The max results per page is configurable with the parameter `max_per_page`,
> as default it will return 100 (that is the max of most of the endpoints)

### Rate limiting

GitHub's primary rate limit (5,000 requests/hour, 15,000 for GitHub Enterprise Cloud users)
is per **account**, not per token: every token, OAuth app or GitHub App acting as that
account draws from the same hourly quota. The `X-RateLimit-Remaining` header the tap reads
therefore reflects the whole account, including requests made by other tools with other tokens.

- `min_remain_rate_limit` (default `0`): the tap stops and waits for the window to reset as
  soon as the account's remaining quota reaches this value. This is the share of the hourly
  quota left for every *other* consumer of the account, so when the token belongs to a shared
  bot account set it well above `0` (e.g. `1000`); with `0` the tap is allowed to use the
  whole quota and starve the other consumers.
- `max_sleep_seconds` (default `3700`): longest wait the tap accepts before failing with
  `RateLimitSleepExceeded`, both when it reaches `min_remain_rate_limit` after a successful
  request and when GitHub rejects a request. The default is slightly more than one rate limit
  window (the wait is `X-RateLimit-Reset` plus a 15 second margin), so the tap always waits for
  the next window; lower it to fail fast and let the scheduler retry from the bookmark.
- When GitHub rejects a request outright (`403`/`429`), typically because another consumer
  drained the account, the tap follows GitHub's guidance: wait `Retry-After` if present, else
  wait for `X-RateLimit-Reset` when `X-RateLimit-Remaining` is `0`, else one minute. Then it
  retries.
- Every response's rate limit headers are logged at `DEBUG` level (`Rate limit: X-RateLimit-Limit=...`).
  Singer logs at `INFO` by default; point `LOGGING_CONF_FILE` at a copy of singer-python's
  `logging.conf` with `level=DEBUG` to see them. Since the values are account-wide, a jump in
  `X-RateLimit-Used` between two consecutive tap requests is another consumer of the account.

4. Run the tap in discovery mode to get properties.json file

    ```bash
    tap-github --config config.json --discover > properties.json
    ```
5. In the properties.json file, select the streams to sync

    Each stream in the properties.json file has a "schema" entry.  To select a stream to sync, add `"selected": true` to that stream's "schema" entry.  For example, to sync the pull_requests stream:
    ```
    ...
    "tap_stream_id": "pull_requests",
    "schema": {
      "selected": true,
      "properties": {
        "updated_at": {
          "format": "date-time",
          "type": [
            "null",
            "string"
          ]
        }
    ...
    ```

6. Run the application

    `tap-github` can be run with:

    ```bash
    tap-github --config config.json --properties properties.json
    ```

---

Copyright &copy; 2018 Stitch
