CREATE OR REPLACE VIEW test_view AS
  SELECT
    1 AS col_a,
    2 AS col_b,
    3 AS col_c;

COMMENT ON COLUMN test_view.col_a IS E'comment a';
COMMENT ON COLUMN test_view.col_b IS E'comment b';

CREATE OR REPLACE VIEW test_view_changed AS
  SELECT
    1 AS col_a,
    2 AS col_b,
    3 AS col_c,
    4 AS col_d;

COMMENT ON COLUMN test_view_changed.col_a IS E'comment a';
COMMENT ON COLUMN test_view_changed.col_b IS E'comment b';

CREATE OR REPLACE VIEW test_view_created AS
  SELECT
    1 AS col_a,
    2 AS col_b,
    3 AS col_c,
    4 AS col_d;

COMMENT ON COLUMN test_view_created.col_a IS E'comment a';
COMMENT ON COLUMN test_view_created.col_b IS E'comment b';