DROP VIEW IF EXISTS "public"."test_view_changed";
COMMENT ON COLUMN "public"."test_view".col_a  IS 'comment a';
COMMENT ON COLUMN "public"."test_view".col_b  IS 'comment b';
CREATE OR REPLACE VIEW "public"."test_view_changed" AS  SELECT 1 AS col_a,
    2 AS col_b,
    3 AS col_c,
    4 AS col_d;
COMMENT ON COLUMN "public"."test_view_changed".col_a  IS 'comment a';
COMMENT ON COLUMN "public"."test_view_changed".col_b  IS 'comment b';
CREATE OR REPLACE VIEW "public"."test_view_created" AS  SELECT 1 AS col_a,
    2 AS col_b,
    3 AS col_c,
    4 AS col_d;
COMMENT ON COLUMN "public"."test_view_created".col_a  IS 'comment a';
COMMENT ON COLUMN "public"."test_view_created".col_b  IS 'comment b';
ALTER VIEW IF EXISTS "public"."test_view_changed" OWNER TO dev;
COMMENT ON VIEW "public"."test_view_changed"  IS NULL;
ALTER VIEW IF EXISTS "public"."test_view_created" OWNER TO dev;
COMMENT ON VIEW "public"."test_view_created"  IS NULL;