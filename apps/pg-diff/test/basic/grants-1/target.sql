CREATE TABLE post (
    id         INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    author_id  INTEGER
); 

GRANT SELECT, UPDATE, DELETE, INSERT ON post TO dev_pg_diff_old;
GRANT SELECT, UPDATE, DELETE, INSERT ON post TO dev_pg_diff_part;

CREATE TABLE comment (
    id           INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    author_id    INTEGER,
    revoke       INTEGER
);

GRANT SELECT, UPDATE (revoke), DELETE, INSERT ON comment TO dev_pg_diff;

CREATE TABLE add_column_grants (
    id           INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    author_id    INTEGER
);
GRANT SELECT, UPDATE, DELETE, INSERT ON add_column_grants TO dev_pg_diff;