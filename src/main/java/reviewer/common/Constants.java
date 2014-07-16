package reviewer.common;

/**
 * Created by sandeep on 7/16/14.
 */
public class Constants {
    // Initialize some common constants and variables
    public static final String BOOK_NAMES_CSV = "Atlas Shrugged, Autbiography of a Yogi, Fountain Head,"
            + " My Experiments with Truth, We The People, Autobiography of Swamy Vivekananda";

    public static final int MAX_REVIEWS_PER_USER = 25;

    // handy, rather than typing this out several times
    public static final String HORIZONTAL_RULE = "----------" + "----------"
            + "----------" + "----------" + "----------" + "----------"
            + "----------" + "----------" + "\n";

    public static final char[] PERMISSIBLE_EMAIL_ID_CHARACTERS = {
            'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R',
            'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '_'};

    public static final char[] PERMISSIBLE_REVIEW_CHARACTERS = {
            'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R',
            'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '_', '.', ' ', ';'};

    public static final char[] PERMISSIBLE_EMAIL_DOMAIN_CHARACTERS = {
            'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R',
            'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',};

    public static final char[] PERMISSIBLE_DOMAIN_EXT_CHARACTERS = {
            'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R',
            'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',};
}
