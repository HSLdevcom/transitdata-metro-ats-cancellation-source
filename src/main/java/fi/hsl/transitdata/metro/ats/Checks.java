package fi.hsl.transitdata.metro.ats;

import java.util.Collection;

import static org.apache.commons.lang3.StringUtils.isBlank;

public class Checks {

    public static <T, C extends Collection<T>> C checkNotEmpty(String paramName, C collection) {
        if (collection == null) {
            throw new IllegalArgumentException(paramName + " must not be null");
        }
        if (collection.isEmpty()) {
            throw new IllegalArgumentException(paramName + " must not be empty");
        }

        return collection;
    }

    public static String checkNotEmpty(String paramName, String value) {
        if (value == null) {
            throw new IllegalArgumentException(paramName + " must not be null");
        }
        if (value.isEmpty() || isBlank(value)) {
            throw new IllegalArgumentException(paramName + " must not be empty");
        }

        return value;
    }

    public static void checkEither(boolean first, boolean second, String errorMessage) {
        if (first == second) {
            throw new IllegalArgumentException(errorMessage);
        }
    }
}
