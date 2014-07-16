-- books table holds the books ids and names
CREATE TABLE books
(
  book_id integer     NOT NULL
, book_name   varchar(50) NOT NULL
, CONSTRAINT PK_books PRIMARY KEY
  (
    book_id
  )
);

-- reviews table holds every valid review.
-- reviewers are not allowed to submit more than <x> reviews, x is passed to client application-- reviewer is identified by email
CREATE TABLE reviews
(
  email       varchar(50)     NOT NULL
, review      varchar(100) NOT NULL
, book_id  integer    NOT NULL
);

PARTITION TABLE reviews ON COLUMN email;

-- rollup of reviews by email
CREATE VIEW v_reviews_by_email
(
  email,
  num_reviews
)
AS
   SELECT email,  COUNT(*)
     FROM reviews
 GROUP BY email;


-- stored procedures
CREATE PROCEDURE FROM CLASS reviewer.procedures.Initialize;
CREATE PROCEDURE FROM CLASS reviewer.procedures.Results;
CREATE PROCEDURE FROM CLASS reviewer.procedures.Review;
PARTITION PROCEDURE Review ON TABLE reviews COLUMN email;
