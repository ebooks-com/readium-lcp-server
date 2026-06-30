-- Column types below mirror the production schema (the dbo.license / dbo.content views
-- over the ebooks.License / ebooks.Content base tables). The varchar/nvarchar split is
-- deliberate and load-bearing: license.id is varchar, while content.id and
-- license.content_fk are nvarchar. The Go code binds id parameters accordingly
-- (dbutils.VarcharParam wraps only the varchar license.id predicates) so SQL Server
-- performs index seeks instead of CONVERT_IMPLICIT scans. Keep these types in sync.

-- varchar to match the varchar license.id column it is joined against (license.GetByIDs).
IF TYPE_ID(N'LicenseIDType') IS NULL
CREATE TYPE LicenseIDType AS TABLE (id varchar(255) NOT NULL);
GO

CREATE TABLE content (
    id nvarchar(255) PRIMARY KEY NOT NULL, -- nvarchar in production (ebooks.Content.Id)
    encryption_key varbinary(64) NOT NULL,
    location text NOT NULL,
    length bigint,
    sha256 varchar(64),
    type varchar(255)
);

CREATE TABLE license (
    id varchar(255) PRIMARY KEY NOT NULL,
    user_id varchar(255) NOT NULL,
    provider varchar(255) NOT NULL,
    issued datetime NOT NULL,
    updated datetime DEFAULT NULL,
    rights_print bigint DEFAULT NULL,
    rights_copy bigint DEFAULT NULL,
    rights_start datetime DEFAULT NULL,
    rights_end datetime DEFAULT NULL,
    content_fk nvarchar(255) NOT NULL, -- nvarchar in production (ebooks.License.ContentId)
    lsd_status tinyint default 0,
    FOREIGN KEY(content_fk) REFERENCES content(id)
);