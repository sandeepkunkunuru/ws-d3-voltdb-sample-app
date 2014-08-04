Book Reviews
============

Description
-----------
This application has 2 tables : books and reviews  and 4 procedures : Initialize, Review, Results and ReviewsForBook.

### Tables

Books tables holds books and reviews tables hold reviews against those
books given by reviewers identified by unique email ids.

### Procedures

- Initialize - Initializes the books table with 6 books
- Review - Allows you to create a review after doing following validations.
    - If the review is for a valid book.
    - If the reviewer has reviewed more books than permissible.
- Results - Selects the top book based on number of reviews
- ReviewsForBooks - Gives the count of reviews for a given book.

Reference projects
-------------------
- Voltdb Voter sample application - https://github.com/VoltDB/voltdb/tree/master/examples/voter
- D3/Websockets Experiment - https://github.com/tjmw/js-D3-Websockets-Experiment
- Java EE 7 - GlassFish WebSockets Chat Tutorial 
    - https://bitbucket.org/hascode/javaee7-websocket-chat
    - http://www.hascode.com/2013/08/creating-a-chat-application-using-java-ee-7-websockets-and-glassfish-4/

 