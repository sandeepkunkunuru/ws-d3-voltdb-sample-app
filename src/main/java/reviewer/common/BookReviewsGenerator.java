package reviewer.common;

import java.util.Random;

public class BookReviewsGenerator {
    public static class Review {
        public final int bookId;
        public final String email;
        public final String review;

        protected Review(int bookId, String email, String review) {
            this.bookId = bookId;
            this.email = email;
            this.review = review;
        }
    }

    private final int bookCount;
    private final Random rand = new Random();

    public BookReviewsGenerator(final int bookCount) {
        this.bookCount = bookCount;
    }

    /**
     * Receives/generates a simulated review
     *
     * @return details (email, review and book to which the review is given)
     */
    public Review receive() {
        // Pick a book number
        int bookId = rand.nextInt(bookCount) + 1;

        //  introduce an invalid book every 100 call or so to simulate fraud
        //  and invalid entries (something the transaction validates against)
        if (rand.nextInt(100) == 0) {
            bookId = 999;
        }

        // Build the email
        String email = RandomTextGenerator.generateText(25, Constants.PERMISSIBLE_EMAIL_ID_CHARACTERS) +
                "@" + RandomTextGenerator.generateText(5, Constants.PERMISSIBLE_EMAIL_DOMAIN_CHARACTERS) +
                "." + RandomTextGenerator.generateText(3, Constants.PERMISSIBLE_DOMAIN_EXT_CHARACTERS);

        // Build review
        String review = RandomTextGenerator.generateText(100, Constants.PERMISSIBLE_REVIEW_CHARACTERS);

        return new Review(bookId, email, review);
    }
}
