-- Update markers and update history, in the alkis_scraper database.
-- Run once:  psql -d alkis_scraper -f sql/state_tables.sql
-- (migrate_state_json.py runs it too.) Safe to run again.

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

-- One row per successful database update, written by main.py once every stage
-- of the run has succeeded.
CREATE TABLE IF NOT EXISTS public.update_history (
    state     text        NOT NULL,
    job       text        NOT NULL,
    started   timestamptz NOT NULL,
    finished  timestamptz NOT NULL,
    elapsed   interval    GENERATED ALWAYS AS (finished - started) STORED,
    PRIMARY KEY (state, job)
);
CREATE INDEX IF NOT EXISTS update_history_state_finished
    ON public.update_history (state, finished);

-- How often each state's database is updated, in days between updates.
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
      FROM public.update_history) t
GROUP BY state;
