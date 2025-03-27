DROP ROLE IF EXISTS dev_pg_diff;
DROP ROLE IF EXISTS dev_pg_diff_old;
DROP ROLE IF EXISTS dev_pg_diff_part;
CREATE ROLE dev_pg_diff;
CREATE ROLE dev_pg_diff_old;
CREATE ROLE dev_pg_diff_part;

CREATE TABLE post (
    id           INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    author_id    INTEGER
);

GRANT SELECT, UPDATE, DELETE, INSERT ON post TO dev_pg_diff;
GRANT SELECT, UPDATE, DELETE ON post TO dev_pg_diff_part;