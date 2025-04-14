package com.linkedin.venice.pubsub;

import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntMaps;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A utility class that maps fully qualified class names of {@code PubSubPosition} implementations
 * to corresponding integer type IDs and vice versa.
 * <p>
 * This mapping is used to serialize and deserialize {@code PubSubPosition} instances in a compact form.
 * The constructor verifies that all provided class names are valid and can be loaded at runtime.
 */
public class PubSubPositionTypeRegistry {
  public static final Logger LOGGER = LogManager.getLogger(PubSubPositionTypeRegistry.class);

  /**
   * Reserved type IDs for internal standard {@link PubSubPosition} implementations.
   * <p>
   * These constants define special type IDs used to identify symbolic or built-in PubSub positions.
   * They are reserved for internal use and must not be reused, overridden, or assigned to custom
   * {@link PubSubPosition} implementations by client code.
   * <p>
   * Reserved IDs:
   * <ul>
   *   <li>{@code -2} —> {@link PubSubPosition#EARLIEST}: Marker for the earliest retrievable position in a partition</li>
   *   <li>{@code -1} —> {@link PubSubPosition#LATEST}: Marker for the latest retrievable position in a partition</li>
   *   <li>{@code  0} —> {@link ApacheKafkaOffsetPosition}: Standard Kafka offset-based position</li>
   * </ul>
   * <p>
   * {@link #POSITION_TYPE_INVALID_MAGIC_VALUE} is used as a sentinel value to indicate that a position
   * type ID has not yet been assigned or initialized. It must never be used as a valid position type.
   */
  public static final int EARLIEST_POSITION_RESERVED_TYPE_ID = -2;
  public static final int LATEST_POSITION_RESERVED_TYPE_ID = -1;
  public static final int APACHE_KAFKA_OFFSET_POSITION_TYPE_ID = 0;
  public static final int POSITION_TYPE_INVALID_MAGIC_VALUE = Integer.MIN_VALUE;

  /**
   * A predefined map of reserved PubSub position type IDs to their corresponding fully qualified class names.
   * <p>
   * This map is intended for internal use to bootstrap or validate known position implementations.
   * It includes all reserved positions that are part of the standard Venice PubSub position model.
   * <p>
   * Entries:
   * <ul>
   *   <li>{@code -2} -> {@link PubSubPosition#EARLIEST}</li>
   *   <li>{@code -1} -> {@link PubSubPosition#LATEST}</li>
   *   <li>{@code  0} -> {@link ApacheKafkaOffsetPosition}</li>
   * </ul>
   */
  public static final Int2ObjectMap<String> RESERVED_POSITION_TYPE_ID_TO_CLASS_NAME_MAP;

  static {
    Int2ObjectMap<String> tempMap = new Int2ObjectOpenHashMap<>();
    tempMap.put(EARLIEST_POSITION_RESERVED_TYPE_ID, PubSubPosition.EARLIEST.getClass().getName());
    tempMap.put(LATEST_POSITION_RESERVED_TYPE_ID, PubSubPosition.LATEST.getClass().getName());
    tempMap.put(APACHE_KAFKA_OFFSET_POSITION_TYPE_ID, ApacheKafkaOffsetPosition.class.getName());
    // Make the map unmodifiable for safety
    RESERVED_POSITION_TYPE_ID_TO_CLASS_NAME_MAP = Int2ObjectMaps.unmodifiable(tempMap);
  }

  /**
   * A default, pre-configured {@link PubSubPositionTypeRegistry} instance that contains all reserved
   * {@link PubSubPosition} type IDs and their associated class names.
   * <p>
   * This instance should be used wherever a standard, system-defined position registry is sufficient.
   * It includes entries for known symbolic positions like {@link PubSubPosition#EARLIEST},
   * {@link PubSubPosition#LATEST}, and standard implementations such as {@link ApacheKafkaOffsetPosition}.
   * <p>
   * Note: This instance is read-only and should not be modified or extended at runtime.
   */
  public static final PubSubPositionTypeRegistry RESERVED_POSITION_TYPE_REGISTRY =
      new PubSubPositionTypeRegistry(RESERVED_POSITION_TYPE_ID_TO_CLASS_NAME_MAP);

  private final Object2IntMap<String> classNameToTypeIdMap;
  private final Int2ObjectMap<String> typeIdToClassNameMap;

  /**
   * Constructs a {@link PubSubPositionTypeRegistry} by merging the provided type ID to class name map
   * with the system-defined reserved position type mappings.
   * <p>
   * Reserved position type IDs (such as those for {@link PubSubPosition#EARLIEST}, {@link PubSubPosition#LATEST}, and
   * {@link ApacheKafkaOffsetPosition}) are validated to ensure they are not overridden with a conflicting class name.
   * If a reserved ID is present in the provided map, it must map to the exact same class name as the reserved definition;
   * otherwise, an {@link IllegalArgumentException} is thrown.
   * <p>
   * Any reserved IDs not present in the input map are automatically included.
   * The final registry is unmodifiable after construction.
   *
   * @param userProvidedMap a map from position type IDs to fully qualified class names
   * @throws IllegalArgumentException if a reserved type ID is overridden with a different class
   * @throws ClassNotFoundException if any of the provided class names cannot be loaded
   */
  public PubSubPositionTypeRegistry(Int2ObjectMap<String> userProvidedMap) {
    Int2ObjectMap<String> mergedMap = mergeWithReservedTypes(userProvidedMap);
    this.typeIdToClassNameMap = Int2ObjectMaps.unmodifiable(mergedMap);
    this.classNameToTypeIdMap = Object2IntMaps.unmodifiable(getClassNameToTypeIdMap(this.typeIdToClassNameMap));
  }

  /**
   * Merges the given user-provided map with the reserved type map.
   * Throws an exception if any reserved type ID is associated with a different class name.
   *
   * @param userMap the input map to validate and merge
   * @return a new map that contains both reserved and user-defined entries
   */
  private static Int2ObjectMap<String> mergeWithReservedTypes(Int2ObjectMap<String> userMap) {
    Int2ObjectMap<String> merged = new Int2ObjectOpenHashMap<>(userMap);

    for (Int2ObjectMap.Entry<String> reservedEntry: RESERVED_POSITION_TYPE_ID_TO_CLASS_NAME_MAP.int2ObjectEntrySet()) {
      int reservedId = reservedEntry.getIntKey();
      String reservedClassName = reservedEntry.getValue();

      if (!merged.containsKey(reservedId)) {
        // If the reserved ID is not in the user map, add it to the merged map and continue with the next entry
        merged.put(reservedId, reservedClassName);
        continue;
      }

      // If the reserved ID is in the user map, check for conflicts
      String userClassName = merged.get(reservedId);
      if (!reservedClassName.equals(userClassName)) {
        throw new IllegalArgumentException(
            "Conflicting entry for reserved position type ID " + reservedId + ": expected class name ["
                + reservedClassName + "], but got [" + userClassName + "]");
      }
    }
    return merged;
  }

  private static Object2IntMap<String> getClassNameToTypeIdMap(Int2ObjectMap<String> typeIdToClassNameMap) {
    Object2IntMap<String> classNameToTypeIdMap = new Object2IntOpenHashMap<>(typeIdToClassNameMap.size());
    for (Map.Entry<Integer, String> entry: typeIdToClassNameMap.int2ObjectEntrySet()) {
      String className = entry.getValue();
      int typeId = entry.getKey();

      if (StringUtils.isBlank(className)) {
        LOGGER.error(
            "Class name for pubsub position type ID: {} is null. Type ID mapping: {} cannot be used.",
            typeId,
            typeIdToClassNameMap);
        throw new IllegalArgumentException("Class name for type ID: " + typeId + " is null or empty.");
      }

      classNameToTypeIdMap.put(className, typeId);

      try {
        Class.forName(className);
      } catch (ClassNotFoundException e) {
        LOGGER.error("Class not found for FQCN: {} (type ID: {}). Mapping cannot be used.", className, typeId, e);
        throw new IllegalArgumentException("Class not found for fully qualified name: " + className, e);
      }
    }
    return classNameToTypeIdMap;
  }

  /**
   * Returns the integer type ID for the given class name.
   *
   * @param className the fully qualified class name of a {@code PubSubPosition} implementation
   * @return the integer type ID, or -1 if the class name is not found
   */
  public int getTypeId(String className) {
    if (!classNameToTypeIdMap.containsKey(className)) {
      LOGGER.error(
          "PubSub position class name not found: {}. Valid class names: {}",
          className,
          classNameToTypeIdMap.keySet());
      throw new IllegalArgumentException("PubSub position class name not found: " + className);
    }
    return classNameToTypeIdMap.getInt(className);
  }

  /**
   * Returns the integer type ID for the given {@code PubSubPosition} implementation.
   *
   * @param pubSubPosition the {@code PubSubPosition} implementation
   * @return the integer type ID, or -1 if the class name is not found
   */
  public int getTypeId(PubSubPosition pubSubPosition) {
    if (pubSubPosition == null) {
      LOGGER.error("PubSub position is null. Cannot get type ID.");
      throw new IllegalArgumentException("PubSub position is null.");
    }
    return getTypeId(pubSubPosition.getClass().getName());
  }

  /**
   * Checks if the given class name is present in the mapping.
   *
   * @param className the fully qualified class name of a {@code PubSubPosition} implementation
   * @return true if the class name is present, false otherwise
   */
  public boolean hasType(String className) {
    return classNameToTypeIdMap.containsKey(className);
  }

  /**
   * Returns the fully qualified class name for the given integer type ID.
   *
   * @param typeId the integer type ID of a {@code PubSubPosition} implementation
   * @return the fully qualified class name, or null if the type ID is not found
   */
  public String getClassName(int typeId) {
    if (!typeIdToClassNameMap.containsKey(typeId)) {
      LOGGER.error("PubSub position type ID not found: {}. Valid type IDs: {}", typeId, typeIdToClassNameMap.keySet());
      throw new IllegalArgumentException("PubSub position type ID not found: " + typeId);
    }
    return typeIdToClassNameMap.get(typeId);
  }

  /**
   * Returns the mapping of integer type IDs to fully qualified class names.
   * @return a map from integer type IDs to the fully qualified class names of {@code PubSubPosition} implementations
   */
  public Int2ObjectMap<String> getTypeIdToClassNameMap() {
    return typeIdToClassNameMap;
  }

  @Override
  public String toString() {
    return "PositionMapper(" + classNameToTypeIdMap + ")";
  }
}
