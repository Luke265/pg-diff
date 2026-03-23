CREATE TABLE post (
  id_a          INTEGER,
  id_B          INTEGER
);

CREATE FUNCTION custom_function(
) RETURNS integer
  STABLE
  SECURITY DEFINER
  SET search_path TO pg_catalog, public, pg_temp
  LANGUAGE plpgsql
AS
$$
BEGIN
  RETURN 1;
END
$$;

CREATE POLICY _100_select
  ON post
  AS PERMISSIVE
  FOR SELECT
  TO dev_pg_diff
  USING (
  (
    SELECT custom_function()
    ) IN (id_a, id_b) OR id_a IN (SELECT 1)
  );