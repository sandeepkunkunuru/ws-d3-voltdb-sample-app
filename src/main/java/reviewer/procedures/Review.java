package reviewer.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

/**
 * Accepts a review, enforcing business logic: make sure the review is for a valid
 * book and that the reviewer (email of the reviewer) is not above the
 * number of allowed reviews.
 */
public class Review extends VoltProcedure {

    // potential return codes
    public static final long REVIEW_SUCCESSFUL = 0;
    public static final long ERR_INVALID_BOOK = 1;
    public static final long ERR_REVIEWER_OVER_REVIEW_LIMIT = 2;

    // Checks if the review is for a valid book
    public final SQLStmt checkBookStmt = new SQLStmt(
            "SELECT book_id FROM books WHERE book_id = ?;");

    // Checks if the reviewer has exceeded their allowed number of reviews
    public final SQLStmt checkReviewerStmt = new SQLStmt(
            "SELECT num_reviews FROM v_reviews_by_email WHERE email = ?;");

    // Records a review
    public final SQLStmt insertReviewStmt = new SQLStmt(
            "INSERT INTO reviews (email, review, book_id) VALUES (?, ?, ?);");

    public long run(String email, String review, int bookId, long maxReviewsPerEmail) {
        // Queue up validation
        voltQueueSQL(checkBookStmt, EXPECT_ZERO_OR_ONE_ROW, bookId);
        voltQueueSQL(checkReviewerStmt, EXPECT_ZERO_OR_ONE_ROW, email);
        VoltTable validation[] = voltExecuteSQL();

        if (validation[0].getRowCount() == 0) {
            return ERR_INVALID_BOOK;
        }

        if ((validation[1].getRowCount() == 1) &&
                (validation[1].asScalarLong() >= maxReviewsPerEmail)) {
            return ERR_REVIEWER_OVER_REVIEW_LIMIT;
        }

        // Post the review
        voltQueueSQL(insertReviewStmt, EXPECT_SCALAR_MATCH(1), email, review, bookId);
        voltExecuteSQL(true);

        // Set the return value to 0: successful review
        return REVIEW_SUCCESSFUL;
    }
}
