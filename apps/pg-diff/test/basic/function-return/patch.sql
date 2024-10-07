DROP FUNCTION IF EXISTS "public"."tg_test"();
CREATE OR REPLACE FUNCTION public.tg_test()
 RETURNS text
 LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN '1';
END;
$function$;
ALTER FUNCTION "public"."tg_test"() OWNER TO dev;