ALTER TABLE IF EXISTS "public"."post" ADD COLUMN IF NOT EXISTS is_custom BOOLEAN NULL DEFAULT true;
ALTER TABLE IF EXISTS "public"."post" ADD COLUMN IF NOT EXISTS required_col BOOLEAN NULL;
ALTER TABLE IF EXISTS "public"."post" ADD COLUMN IF NOT EXISTS computed_col numeric(16,6) GENERATED ALWAYS AS (author_id) STORED;
CREATE OR REPLACE FUNCTION public.tg_test()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
BEGIN
    -- DO NOTHING
    RETURN NEW;
END;
$function$;
ALTER FUNCTION "public"."tg_test"() OWNER TO dev;
CREATE TRIGGER _110_reset_action_triggers
    AFTER UPDATE OF is_custom
    ON "public"."post"
    FOR EACH ROW
    WHEN ((new.is_custom <> old.is_custom))
    EXECUTE FUNCTION tg_test();