/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.novarocks.tck;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.datasketches.common.Family;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.UpdateSketch;

/** Manual fixture generator pinned by pom.xml to DataSketches Java 6.2.0. */
public final class GenerateFixtures {
  private static final long DEFAULT_SEED = 9001L;
  private static final long CUSTOM_SEED = 123456789L;

  private GenerateFixtures() {}

  public static void main(final String[] args) throws IOException {
    if (args.length != 1) {
      throw new IllegalArgumentException("usage: GenerateFixtures OUTPUT_DIR");
    }
    final Path root = Paths.get(args[0]);
    Files.createDirectories(root.resolve("theta"));
    Files.createDirectories(root.resolve("hll"));

    theta(root, "java62_quickselect_empty_ordered_v3.sk", Family.QUICKSELECT,
        DEFAULT_SEED, 0, 0, 1, true, false);
    theta(root, "java62_quickselect_k_minus_1_ordered_v3.sk", Family.QUICKSELECT,
        DEFAULT_SEED, 0, 4095, 1, true, false);
    theta(root, "java62_quickselect_k_ordered_v3.sk", Family.QUICKSELECT,
        DEFAULT_SEED, 0, 4096, 1, true, false);
    theta(root, "java62_quickselect_k_plus_1_ordered_v3.sk", Family.QUICKSELECT,
        DEFAULT_SEED, 0, 4097, 1, true, false);
    theta(root, "java62_quickselect_n100000_ordered_v3.sk", Family.QUICKSELECT,
        DEFAULT_SEED, 0, 100000, 1, true, false);
    theta(root, "java62_quickselect_n100000_ordered_v4.sk", Family.QUICKSELECT,
        DEFAULT_SEED, 0, 100000, 1, true, true);
    theta(root, "java62_quickselect_n1000_unordered_v3.sk", Family.QUICKSELECT,
        DEFAULT_SEED, 0, 1000, 1, false, false);
    theta(root, "java62_quickselect_n1000_duplicates_ordered_v3.sk", Family.QUICKSELECT,
        DEFAULT_SEED, 0, 1000, 5, true, false);
    theta(root, "java62_quickselect_n1000_custom_seed_ordered_v3.sk", Family.QUICKSELECT,
        CUSTOM_SEED, 0, 1000, 1, true, false);
    theta(root, "java62_quickselect_overlap_left_ordered_v3.sk", Family.QUICKSELECT,
        DEFAULT_SEED, 0, 1000, 1, true, false);
    theta(root, "java62_quickselect_overlap_right_ordered_v3.sk", Family.QUICKSELECT,
        DEFAULT_SEED, 100, 1000, 1, true, false);
    theta(root, "java62_quickselect_disjoint_right_ordered_v3.sk", Family.QUICKSELECT,
        DEFAULT_SEED, 2000, 1000, 1, true, false);
    theta(root, "java62_alpha_n100000_ordered_v3.sk", Family.ALPHA,
        DEFAULT_SEED, 0, 100000, 1, true, false);

    for (final TgtHllType type : TgtHllType.values()) {
      hll(root, type, 1, 12);
      hll(root, type, 10, 12);
      hll(root, type, 10000, 12);
      hll(root, type, 100000, 12);
    }
    hll(root, TgtHllType.HLL_8, 10000, 10);
  }

  private static void theta(final Path root, final String name, final Family family,
      final long seed, final int start, final int count, final int repetitions,
      final boolean ordered, final boolean compressed) throws IOException {
    final UpdateSketch sketch = UpdateSketch.builder()
        .setFamily(family).setNominalEntries(4096).setSeed(seed).build();
    for (int repetition = 0; repetition < repetitions; repetition++) {
      for (int value = start; value < start + count; value++) {
        sketch.update(value);
      }
    }
    final CompactSketch compact = sketch.compact(ordered, null);
    final byte[] bytes = compressed ? compact.toByteArrayCompressed() : compact.toByteArray();
    Files.write(root.resolve("theta").resolve(name), bytes);
  }

  private static void hll(final Path root, final TgtHllType type, final int count,
      final int lgK) throws IOException {
    final HllSketch sketch = new HllSketch(lgK, type);
    for (int value = 0; value < count; value++) {
      sketch.update(value);
    }
    final String typeName = type.name().toLowerCase().replace('_', '-');
    final String name = "java62_" + typeName + "_lgk" + lgK + "_n" + count + ".sk";
    Files.write(root.resolve("hll").resolve(name), sketch.toCompactByteArray());
  }
}
