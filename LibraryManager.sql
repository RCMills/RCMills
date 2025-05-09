USE master;
GO

DROP DATABASE IF EXISTS Library_Manager;
GO

CREATE DATABASE Library_Manager;
GO

USE Library_Manager;
GO

DROP TABLE IF EXISTS dbo.[Book];
DROP TABLE IF EXISTS dbo.[Borrower];
DROP TABLE IF EXISTS dbo.[Book_Borrower]; -- rename of Borrowing_Status (wouldn't use a verb in a table name)

GO

CREATE TABLE dbo.Book(
	[Book_id] int IDENTITY(1,1) NOT NULL PRIMARY KEY
	, [Book_Name] nvarchar(256) NOT NULL
	, [Book_Author] nvarchar(256) NOT NULL
	, [Book_Subject] nvarchar(max) NULL
);
GO

CREATE TABLE dbo.[Borrower](
	[Borrower_Id] int IDENTITY(1,1) NOT NULL PRIMARY KEY
	, [Borrower_Name] nvarchar(256) NOT NULL
	, [Borrower_Email] nvarchar(256) NOT NULL
	, [Borrower_Phone] varchar(20) NULL
)
GO

/*
	The Book_Borrower table has a PK that is an "Autonumber" or Identity. 
	The reason this is done it to ensure the table always fills new records at the bottom on the table.
	Q. What about that is important?  
	A. The indexes are built as binary trees, each next level is related to only one parent. As such, adding rows to the table
		could add levels in the middle of an index, this this case the PK which is how the table is built on disk. So, in order
		to ensure the table is always writing to the end of itself on disk, use this method.  

	An index could also be added to cover (A covering index) to Book_Id and Borrower_Id to help searching for a book or borrower,
	and to that end as an include value to that index the Checkout_Date to avoid a key lookup.

	CREATE INDEX IX_Book_Borrower_Cover on dbo.[Book_Borrower] ([Book_Id], [Borrower_Id]) INCLUDE([Checkout_Date])

*/

CREATE TABLE dbo.[Book_Borrower](
	[Book_Borrower_Id] BIGINT IDENTITY(1,1) NOT NULL
	, [Book_Id] int NOT NULL
	, [Borrower_Id] int NOT NULL
	, [Checkout_Date] datetime NOT NULL
	, [Checkin_Date] datetime NOT NULL
	CONSTRAINT PK_Book_Borrower PRIMARY KEY CLUSTERED ([Book_Borrower_Id]) ON [PRIMARY]
	, CONSTRAINT UC_Book_Borrower UNIQUE ([Book_Id], [Borrower_Id])
	, CONSTRAINT FK_Book_Borrower_Book_Id FOREIGN KEY ([Book_Id]) REFERENCES dbo.Book([Book_Id])
	, CONSTRAINT FK_Book_Borrower_Borrower_Id FOREIGN KEY ([Borrower_Id]) REFERENCES dbo.Borrower([Borrower_Id])
) ON [PRIMARY]
GO

CREATE OR ALTER FUNCTION dbo.udf_RentalPeriod() RETURNS INT AS BEGIN RETURN (SELECT 14 [RentalPeriod]) END;
GO


CREATE OR ALTER PROCEDURE dbo.[usp_R_BookStatus]
(
	@BookId int = NULL
)
AS

	DECLARE @RentalPeriod int = dbo.udf_RentalPeriod()

	/*
		The concept of status is derived at call time to ensure the status can never be empty or incorrect
		storing such values can lead to mismatches in value versus reality (Where have you seen such things before???)
	*/

	SELECT 
		b.Book_Name
		, b.Book_Author
		, b.Book_Subject
		, CASE WHEN bb.Checkout_Date <= DATEADD(DAY, @RentalPeriod, bb.Checkout_Date) THEN 'Checked_Out'
			WHEN bb.Checkout_Date > DATEADD(DAY, @RentalPeriod, bb.Checkout_Date) THEN 'Overdue'
			WHEN bb.Checkout_Date IS NULL THEN 'Available'
			END [Book_Status]
	-- These values will comeback NULL if the book is avaiable
		, CASE WHEN bb.Checkout_Date IS NOT NULL THEN DATEADD(DAY, @RentalPeriod, bb.Checkout_Date) END [Expected_Checkin]
		, bb.Checkout_Date
		, bb.Borrower_Id
		, br.Borrower_Name
		, br.Borrower_Phone
		, br.Borrower_Email
	FROM	
		dbo.Book b
	LEFT OUTER JOIN (SELECT 
						Book_Id
						, Borrower_Id
						, MAX(Checkout_Date) [Checkout_Date] 
					FROM 
						dbo.Book_Borrower
					WHERE
						Checkin_Date IS NULL -- Filter the result to only show checked out books in this select
					GROUP BY 
						Book_Id, Borrower_Id) bb ON
		bb.Book_Id = b.Book_id
	LEFT OUTER JOIN dbo.Borrower br ON
		br.Borrower_Id = bb.Borrower_Id
	WHERE
	-- If you pass no book Id then you get a status for all books
	    (b.Book_id IS NULL OR b.Book_id = @BookId)

GO

CREATE OR ALTER PROCEDURE dbo.[usp_C_CheckoutBook]
(
	@Book_Id int
	, @Borrower_Id int
)
AS
BEGIN

	DECLARE @Today datetime = Getdate()

	-- Rule - Borrower cannot exceed three books at any given point in time
	IF (SELECT COUNT(*) FROM dbo.Book_Borrower WHERE Borrower_Id = @Borrower_Id AND Checkin_Date IS NULL) >= 3
	BEGIN

		DECLARE @Message nvarchar(2048)

		SELECT @Message = N'Borrower Cannot have more than 3 books checked out any any given time';

		THROW 60000, @Message, 1;
		
		RETURN 

	END

	-- the same book cannot be checked out by more than once at a time

	IF NOT EXISTS(SELECT * FROM dbo.Book_Borrower WHERE Book_Id = @Book_Id AND Checkin_Date IS NULL)
	BEGIN

		INSERT dbo.Book_Borrower
			(Book_Id 
			, Borrower_Id
			, Checkout_Date)
		VALUES(
			@Book_Id
			, @Borrower_Id
			, @Today)
					   
	END

	SELECT DATEADD(DAY, dbo.udf_RentalPeriod(), GETDATE()) [Expected_Return_Date]

END
GO

CREATE OR ALTER PROCEDURE dbo.usp_C_CheckinBook
(
	@bookId int
)
AS

	UPDATE 
		dbo.Book_Borrower 
	SET 
		Checkin_Date = GETDATE() 
	WHERE 
		Book_Id = @bookId 
	AND Checkin_Date IS NULL;

GO

