CREATE TABLE Product
(
	ID BIGINT PRIMARY KEY,
	Name VARCHAR(500),
	[Description] VARCHAR(500),
)

CREATE TABLE ProductImage
(
	ProductId INT,
	Url VARCHAR(512),
	Idx INT
)