package com.adhoc.flight.utils;

/**
 * Hold some strings used when printing results.
 */
enum Fillers {
  HEADER("------------------"),
  FOOTER("==================");

  private final String string;

  Fillers(String string) {
    this.string = string;
  }

  /**
   * Get the representation of this instace, formatted as String.
   *
   * @return the {@code String} representation of this instance, formatted.
   */
  public final String toFormattedString() {
    return string;
  }
}