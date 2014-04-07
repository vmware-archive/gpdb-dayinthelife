CREATE OR REPLACE FUNCTION myFunc (numtimes integer, msg text)
  RETURNS text AS
$BODY$
DECLARE
    strresult text;
BEGIN
    strresult := '';
    IF numtimes = 1 THEN
        strresult := 'Only one row!';
    ELSIF numtimes > 0 AND numtimes < 11 THEN
        FOR i IN 1 .. numtimes LOOP
            strresult := strresult || msg || '; '; --E'\r\n';
        END LOOP;
    ELSE
        strresult := 'You can not do that.';
        IF numtimes <= 0 THEN
            strresult := strresult || ' Must be greater than zero.';
        ELSIF numtimes > 10 THEN
            strresult := strresult || ' That''s too many items!';
        END IF;
    END IF;
    RETURN strresult;
END;
$BODY$
  LANGUAGE 'plpgsql' IMMUTABLE;
ALTER FUNCTION myFunc(integer, text) OWNER TO gpadmin;
