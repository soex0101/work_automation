-- Update markers and update history, in the alkis_scraper database.
-- state_db.py runs this on first use if any of the objects below is missing, so
-- nothing has to be set up by hand. Safe to run again.

-- One marker per state and job: what that job's download saw on the portal.
-- check_update.py compares a portal against each state's newest row. Written
-- after the download (before the swap for by / st), so a row here means the data
-- was fetched, not that the database was updated -- that is update_history.
CREATE TABLE IF NOT EXISTS public.update_state (
    state     text        NOT NULL,
    job       text        NOT NULL,
    marker    jsonb       NOT NULL,   -- a dict of one entry per file, or a single string
    recorded  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (state, job)
);

-- One row per main.py run, written as PROCESSING when it starts and set to
-- COMPLETE once every stage has succeeded, or FAILED with the reason in
-- error_message. finished (and so elapsed) stays NULL while PROCESSING; a run
-- killed outright (SIGKILL, reboot) is left PROCESSING.
-- pid numbers the runs in the order they started; (state, job) stays unique,
-- since that is what state_db.py's upsert matches on.
CREATE TABLE IF NOT EXISTS public.update_history (
    pid            bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    state          text        NOT NULL,
    job            text        NOT NULL,
    started        timestamptz NOT NULL,
    finished       timestamptz,
    elapsed        interval    GENERATED ALWAYS AS (finished - started) STORED,
    status         text        NOT NULL DEFAULT 'PROCESSING'
                   CHECK (status IN ('PROCESSING', 'COMPLETE', 'FAILED')),
    error_message  text,
    UNIQUE (state, job)
);
CREATE INDEX IF NOT EXISTS update_history_state_finished
    ON public.update_history (state, finished);

-- How often each state's database is updated, in days between completed updates.
CREATE OR REPLACE VIEW public.update_frequency AS
SELECT state,
       count(*)                                         AS updates,
       min(finished)::date                              AS first_update,
       max(finished)::date                              AS last_update,
       current_date - max(finished)::date               AS days_since_last,
       round(avg(gap), 1)                               AS avg_days_between,
       min(gap)                                         AS min_days_between,
       max(gap)                                         AS max_days_between,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY gap) AS median_days_between
FROM (SELECT state, finished,
             finished::date - lag(finished::date)
                 OVER (PARTITION BY state ORDER BY finished) AS gap
      FROM public.update_history
      WHERE status = 'COMPLETE') t
GROUP BY state;

-- One row per state per check_update.py run: CHECKING while the check runs, then
-- its verdict. Read by the admin panel (alkis-update-manager) to show whether a
-- state needs an update; written by every check, cron's and the panel's alike.
-- A row left CHECKING is a check that died before it finished.
CREATE TABLE IF NOT EXISTS public.update_check (
    cid      bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    state    text        NOT NULL,
    job      text        NOT NULL,           -- the check run's --job-id
    started  timestamptz NOT NULL DEFAULT now(),
    finished timestamptz,
    result   text        NOT NULL DEFAULT 'CHECKING'
             CHECK (result IN ('CHECKING', 'UPDATE_NEEDED', 'UP_TO_DATE', 'FAILED')),
    detail   text,                           -- the check's lines, or the exception
    UNIQUE (state, job)
);
CREATE INDEX IF NOT EXISTS update_check_state_started
    ON public.update_check (state, started);
