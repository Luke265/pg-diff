CREATE OR REPLACE FUNCTION public.tg_test()
 RETURNS INTEGER
 LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN 1;
END;
$function$;