ALTER TABLE IF EXISTS "public"."post" ADD COLUMN IF NOT EXISTS is_custom BOOLEAN NULL DEFAULT true;
ALTER TABLE IF EXISTS "public"."post" ADD COLUMN IF NOT EXISTS required_col BOOLEAN NULL;
ALTER TABLE IF EXISTS "public"."post" ADD COLUMN IF NOT EXISTS computed_col numeric(16,6) GENERATED ALWAYS AS (author_id) STORED;