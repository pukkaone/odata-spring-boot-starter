package com.github.pukkaone.odata.elasticsearch2.provider;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Stream convenience methods.
 */
public final class Streams {

  // Private constructor disallows creating instances of this class.
  private Streams() {
  }

  /**
   * Creates {@link Stream} of elements from {@link Iterable}.
   *
   * @param <T>
   *     element type
   * @param iterable
   *     source of elements
   * @return stream
   */
  public static <T> Stream<T> stream(Iterable<T> iterable) {
    return StreamSupport.stream(iterable.spliterator(), false);
  }
}
