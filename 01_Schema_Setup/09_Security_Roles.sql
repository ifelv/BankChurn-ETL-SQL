/*==========================================================
09_Security_Roles
============================================================*/

CREATE ROLE JuniorAnalystRole;
GO

GRANT SELECT ON SCHEMA::Person TO JuniorAnalystRole;
GRANT SELECT ON SCHEMA::Bank TO JuniorAnalystRole;

DENY DELETE ON SCHEMA::Person TO JuniorAnalystRole;
DENY DELETE ON SCHEMA::Bank TO JuniorAnalystRole;
DENY UPDATE ON SCHEMA::Bank TO JuniorAnalystRole;

CREATE USER [John_Doe] WITHOUT LOGIN;

ALTER ROLE JuniorAnalystRole ADD MEMBER [John_Doe];
GO



CREATE SYNONYM NBS FOR Person.NaiveBayesScore;
GO

SELECT TOP 5 * FROM NBS;

