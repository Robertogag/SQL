CREATE FUNCTION keepcoding.clean_integer(input INT64)
RETURNS INT64
AS (IFNULL(input, -999999));
