package com.github.pukkaone.odata.elasticsearch2.processor;

/**
 * Converts literal value.
 */
public final class LiteralUtils {

  // Private constructor disallows creating instances of this class.
  private LiteralUtils() {
  }

  /**
   * Removes quotes at start and end of literal string.
   *
   * @param input
   *     input string
   * @return unquoted string
   */
  public static String unquote(String input) {
    String value = input;
    if (value.startsWith("'") && value.endsWith("'")) {
      value = value.substring(1, value.length() - 1);
    }

    return value;
  }
}
