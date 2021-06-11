package com.adhoc.flight.utils;

import org.apache.arrow.util.Preconditions;

/**
 * Hold some prefixes used when printing results.
 */
enum Prefixes {
  ERROR("[ERROR]"),
  INFORMATION("[INFO]");

  private final String string;

  Prefixes(String string) {
    Preconditions.checkArgument(string.length() > 0);
    this.string = string;
  }

  /**
   * Get the representation of this instace, formatted as String.
   *
   * @return the {@code String} representation of this instance, formatted.
   */
  public String toFormattedString() {
    return string;
  }
}