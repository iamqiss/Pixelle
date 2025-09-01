/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.operations;

import static java.lang.Double.parseDouble;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static org.neo4j.cypher.operations.CursorUtils.propertyKeys;
import static org.neo4j.values.storable.Values.EMPTY_STRING;
import static org.neo4j.values.storable.Values.FALSE;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.TRUE;
import static org.neo4j.values.storable.Values.booleanValue;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_LIST;
import static org.neo4j.values.virtual.VirtualValues.asList;
import static scala.jdk.javaapi.CollectionConverters.asJava;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.neo4j.cypher.internal.expressions.NormalForm;
import org.neo4j.cypher.internal.runtime.DbAccess;
import org.neo4j.cypher.internal.runtime.ExpressionCursors;
import org.neo4j.cypher.internal.runtime.QueryContext;
import org.neo4j.cypher.internal.runtime.RuntimeNotifier;
import org.neo4j.cypher.internal.util.RuntimeUnsatisfiableRelationshipTypeExpression;
import org.neo4j.cypher.internal.util.symbols.AnyType;
import org.neo4j.cypher.internal.util.symbols.BooleanType;
import org.neo4j.cypher.internal.util.symbols.ClosedDynamicUnionType;
import org.neo4j.cypher.internal.util.symbols.CypherType;
import org.neo4j.cypher.internal.util.symbols.DateType;
import org.neo4j.cypher.internal.util.symbols.DurationType;
import org.neo4j.cypher.internal.util.symbols.FloatType;
import org.neo4j.cypher.internal.util.symbols.GeometryType;
import org.neo4j.cypher.internal.util.symbols.IntegerType;
import org.neo4j.cypher.internal.util.symbols.ListType;
import org.neo4j.cypher.internal.util.symbols.LocalDateTimeType;
import org.neo4j.cypher.internal.util.symbols.LocalTimeType;
import org.neo4j.cypher.internal.util.symbols.MapType;
import org.neo4j.cypher.internal.util.symbols.NodeType;
import org.neo4j.cypher.internal.util.symbols.NothingType;
import org.neo4j.cypher.internal.util.symbols.NullType;
import org.neo4j.cypher.internal.util.symbols.NumberType;
import org.neo4j.cypher.internal.util.symbols.PathType;
import org.neo4j.cypher.internal.util.symbols.PointType;
import org.neo4j.cypher.internal.util.symbols.PropertyValueType;
import org.neo4j.cypher.internal.util.symbols.RelationshipType;
import org.neo4j.cypher.internal.util.symbols.StringType;
import org.neo4j.cypher.internal.util.symbols.ZonedDateTimeType;
import org.neo4j.cypher.internal.util.symbols.ZonedTimeType;
import org.neo4j.exceptions.CypherTypeException;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.exceptions.KernelException;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.impl.schema.vector.VectorSimilarity;
import org.neo4j.kernel.api.vector.VectorCandidate;
import org.neo4j.kernel.api.vector.VectorSimilarityFunction;
import org.neo4j.kernel.impl.util.NodeEntityWrappingNodeValue;
import org.neo4j.storageengine.api.LongReference;
import org.neo4j.token.api.TokenConstants;
import org.neo4j.util.CalledFromGeneratedCode;
import org.neo4j.values.AnyValue;
import org.neo4j.values.ElementIdMapper;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.FloatingPointArray;
import org.neo4j.values.storable.FloatingPointValue;
import org.neo4j.values.storable.IntegralArray;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.NoValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.StringValue;
import org.neo4j.values.storable.TemporalValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueRepresentation;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.RelationshipVisitor;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualPathValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;
import org.neo4j.values.virtual.VirtualValues;

/**
 * This class contains static helper methods for the set of Cypher functions
 */
@SuppressWarnings({"ReferenceEquality"})
public final class CypherFunctions {
    private static final BigDecimal MAX_LONG = BigDecimal.valueOf(Long.MAX_VALUE);
    private static final BigDecimal MIN_LONG = BigDecimal.valueOf(Long.MIN_VALUE);
    private static final String[] POINT_KEYS =
            new String[] {"crs", "x", "y", "z", "longitude", "latitude", "height", "srid"};

    private CypherFunctions() {
        throw new UnsupportedOperationException("Do not instantiate");
    }

    public static AnyValue sin(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue) {
            return doubleValue(Math.sin(((NumberValue) in).doubleValue()));
        } else {
            throw needsNumbers("sin()");
        }
    }

    public static AnyValue asin(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.asin(number.doubleValue()));
        } else {
            throw needsNumbers("asin()");
        }
    }

    public static AnyValue haversin(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue((1.0 - Math.cos(number.doubleValue())) / 2);
        } else {
            throw needsNumbers("haversin()");
        }
    }

    public static AnyValue cos(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.cos(number.doubleValue()));
        } else {
            throw needsNumbers("cos()");
        }
    }

    public static AnyValue cot(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(1.0 / Math.tan(number.doubleValue()));
        } else {
            throw needsNumbers("cot()");
        }
    }

    public static AnyValue acos(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.acos(number.doubleValue()));
        } else {
            throw needsNumbers("acos()");
        }
    }

    public static AnyValue tan(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.tan(number.doubleValue()));
        } else {
            throw needsNumbers("tan()");
        }
    }

    public static AnyValue atan(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.atan(number.doubleValue()));
        } else {
            throw needsNumbers("atan()");
        }
    }

    public static AnyValue atan2(AnyValue y, AnyValue x) {
        if (y == NO_VALUE || x == NO_VALUE) {
            return NO_VALUE;
        } else if (y instanceof NumberValue yNumber && x instanceof NumberValue xNumber) {
            return doubleValue(Math.atan2(yNumber.doubleValue(), xNumber.doubleValue()));
        } else {
            throw needsNumbers("atan2()");
        }
    }

    public static AnyValue ceil(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.ceil(number.doubleValue()));
        } else {
            throw needsNumbers("ceil()");
        }
    }

    public static AnyValue floor(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.floor(number.doubleValue()));
        } else {
            throw needsNumbers("floor()");
        }
    }

    @CalledFromGeneratedCode
    public static AnyValue round(AnyValue in) {
        return round(in, Values.ZERO_INT, Values.stringValue("HALF_UP"), Values.booleanValue(false));
    }

    @CalledFromGeneratedCode
    public static AnyValue round(AnyValue in, AnyValue precision) {
        return round(in, precision, Values.stringValue("HALF_UP"), Values.booleanValue(false));
    }

    public static AnyValue round(AnyValue in, AnyValue precisionValue, AnyValue modeValue) {
        return round(in, precisionValue, modeValue, Values.booleanValue(true));
    }

    public static AnyValue round(AnyValue in, AnyValue precisionValue, AnyValue modeValue, AnyValue explicitModeValue) {
        if (in == NO_VALUE || precisionValue == NO_VALUE || modeValue == NO_VALUE) {
            return NO_VALUE;
        } else if (!(modeValue instanceof StringValue)) {
            throw notAModeString("round", modeValue);
        }

        RoundingMode mode;
        try {
            mode = RoundingMode.valueOf(((StringValue) modeValue).stringValue());
        } catch (IllegalArgumentException e) {
            var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22000)
                    .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N26)
                            .build())
                    .build();
            throw new InvalidArgumentException(
                    gql,
                    "Unknown rounding mode. Valid values are: CEILING, FLOOR, UP, DOWN, HALF_EVEN, HALF_UP, HALF_DOWN, UNNECESSARY.");
        }

        if (in instanceof NumberValue inNumber && precisionValue instanceof NumberValue) {
            int precision = asIntExact(precisionValue, () -> "Invalid input for precision value in function 'round()'");
            boolean explicitMode = ((BooleanValue) explicitModeValue).booleanValue();
            if (precision < 0) {
                throw InvalidArgumentException.negRoundPrecision(precision);
            } else {
                double value = inNumber.doubleValue();
                if (Double.isInfinite(value) || Double.isNaN(value)) {
                    return doubleValue(value);
                }
                /*
                 * For precision zero and no explicit rounding mode, we want to fall back to Java Math.round().
                 * This rounds towards the nearest integer and if there is a tie, towards positive infinity,
                 * which doesn't correspond to any of the rounding modes.
                 */
                else if (precision == 0 && !explicitMode) {
                    return doubleValue(Math.round(value));
                } else {
                    BigDecimal bigDecimal = BigDecimal.valueOf(value);
                    int newScale = Math.min(bigDecimal.scale(), precision);
                    return doubleValue(bigDecimal.setScale(newScale, mode).doubleValue());
                }
            }
        } else {
            throw needsNumbers("round()");
        }
    }

    public static AnyValue abs(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            if (in instanceof IntegralValue) {
                return longValue(Math.abs(number.longValue()));
            } else {
                return doubleValue(Math.abs(number.doubleValue()));
            }
        } else {
            throw needsNumbers("abs()");
        }
    }

    public static AnyValue isNaN(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof FloatingPointValue f) {
            return booleanValue(f.isNaN());
        } else if (in instanceof NumberValue) {
            return BooleanValue.FALSE;
        } else {
            throw needsNumbers("isNaN()");
        }
    }

    public static AnyValue toDegrees(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.toDegrees(number.doubleValue()));
        } else {
            throw needsNumbers("toDegrees()");
        }
    }

    public static AnyValue exp(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.exp(number.doubleValue()));
        } else {
            throw needsNumbers("exp()");
        }
    }

    public static AnyValue log(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.log(number.doubleValue()));
        } else {
            throw needsNumbers("log()");
        }
    }

    public static AnyValue log10(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.log10(number.doubleValue()));
        } else {
            throw needsNumbers("log10()");
        }
    }

    public static AnyValue toRadians(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.toRadians(number.doubleValue()));
        } else {
            throw needsNumbers("toRadians()");
        }
    }

    @CalledFromGeneratedCode
    public static ListValue range(AnyValue startValue, AnyValue endValue) {
        return VirtualValues.range(
                asLong(startValue, () -> "Invalid input for start value in function 'range()'"),
                asLong(endValue, () -> "Invalid input for end value in function 'range()'"),
                1L);
    }

    public static ListValue range(AnyValue startValue, AnyValue endValue, AnyValue stepValue) {
        long step = asLong(stepValue, () -> "Invalid input for step value in function 'range()'");
        if (step == 0L) {
            throw InvalidArgumentException.zeroStepRange();
        }

        return VirtualValues.range(
                asLong(startValue, () -> "Invalid input for start value in function 'range()'"),
                asLong(endValue, () -> "Invalid input for end value in function 'range()'"),
                step);
    }

    @CalledFromGeneratedCode
    public static AnyValue signum(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return longValue((long) Math.signum(number.doubleValue()));
        } else {
            throw needsNumbers("signum()");
        }
    }

    public static AnyValue sqrt(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof NumberValue number) {
            return doubleValue(Math.sqrt(number.doubleValue()));
        } else {
            throw needsNumbers("sqrt()");
        }
    }

    public static DoubleValue rand() {
        return doubleValue(ThreadLocalRandom.current().nextDouble());
    }

    public static TextValue randomUuid() {
        return stringValue(UUID.randomUUID().toString());
    }

    // TODO: Support better calculations, like https://en.wikipedia.org/wiki/Vincenty%27s_formulae
    // TODO: Support more coordinate systems
    public static Value distance(AnyValue lhs, AnyValue rhs) {
        if (lhs instanceof PointValue && rhs instanceof PointValue) {
            return calculateDistance((PointValue) lhs, (PointValue) rhs);
        } else {
            return NO_VALUE;
        }
    }

    public static Value withinBBox(AnyValue point, AnyValue lowerLeft, AnyValue upperRight) {
        if (point instanceof PointValue && lowerLeft instanceof PointValue && upperRight instanceof PointValue) {
            return withinBBox((PointValue) point, (PointValue) lowerLeft, (PointValue) upperRight);
        } else {
            return NO_VALUE;
        }
    }

    public static Value withinBBox(PointValue point, PointValue lowerLeft, PointValue upperRight) {
        CoordinateReferenceSystem crs = point.getCoordinateReferenceSystem();
        if (crs.equals(lowerLeft.getCoordinateReferenceSystem())
                && crs.equals(upperRight.getCoordinateReferenceSystem())) {
            return Values.booleanValue(crs.getCalculator().withinBBox(point, lowerLeft, upperRight));
        } else {
            return NO_VALUE;
        }
    }

    @CalledFromGeneratedCode
    public static Value vectorSimilarityEuclidean(AnyValue lhs, AnyValue rhs) {
        return vectorSimilarity(VectorSimilarity.EUCLIDEAN, lhs, rhs);
    }

    @CalledFromGeneratedCode
    public static Value vectorSimilarityCosine(AnyValue lhs, AnyValue rhs) {
        return vectorSimilarity(VectorSimilarity.COSINE, lhs, rhs);
    }

    public static Value vectorSimilarity(VectorSimilarity similarity, AnyValue lhs, AnyValue rhs) {
        final var function = similarity.latestImplementation();

        if (lhs == NO_VALUE || rhs == NO_VALUE) {
            return NO_VALUE;
        }

        final var a = toFloatArrayVector(function, lhs, "a");
        final var b = toFloatArrayVector(function, rhs, "b");

        if (a.length != b.length) {
            throw new InvalidArgumentException(invalidSimilarityFunctionInputErrorMessage(
                    function, "The supplied vectors do not have the same number of dimensions"));
        }

        return doubleValue(function.compare(a, b));
    }

    public static float[] toFloatArrayVector(VectorSimilarityFunction function, AnyValue arg, String argName) {
        final VectorCandidate candidate = VectorCandidate.maybeFrom(arg);
        if (candidate == null) {
            throw new CypherTypeException(invalidSimilarityFunctionInputErrorMessage(
                    function, "Expected argument %s to be a LIST<INTEGER | FLOAT>".formatted(argName)));
        }

        final float[] floatArray = function.maybeToValidVector(candidate);
        if (floatArray == null) {
            throw new InvalidArgumentException(invalidSimilarityFunctionInputErrorMessage(
                    function, "Argument %s is not a valid vector for this similarity function".formatted(argName)));
        }

        return floatArray;
    }

    private static String invalidSimilarityFunctionInputErrorMessage(VectorSimilarityFunction function, String reason) {
        return "Invalid input for 'vector.similarity.%s()': %s."
                .formatted(function.name().toLowerCase(), reason);
    }

    public static AnyValue startNode(AnyValue anyValue, DbAccess access, RelationshipScanCursor cursor) {
        if (anyValue == NO_VALUE) {
            return NO_VALUE;
        } else if (anyValue instanceof VirtualRelationshipValue rel) {
            return startNode(rel, access, cursor);
        } else {
            throw new CypherTypeException(format(
                    "Invalid input for function 'startNode()': Expected %s to be a RelationshipValue", anyValue));
        }
    }

    public static VirtualNodeValue startNode(
            VirtualRelationshipValue relationship, DbAccess access, RelationshipScanCursor cursor) {
        return VirtualValues.node(relationship.startNodeId(consumer(access, cursor)));
    }

    public static AnyValue endNode(AnyValue anyValue, DbAccess access, RelationshipScanCursor cursor) {
        if (anyValue == NO_VALUE) {
            return NO_VALUE;
        } else if (anyValue instanceof VirtualRelationshipValue rel) {
            return endNode(rel, access, cursor);
        } else {
            throw new CypherTypeException(
                    format("Invalid input for function 'endNode()': Expected %s to be a RelationshipValue", anyValue));
        }
    }

    public static VirtualNodeValue endNode(
            VirtualRelationshipValue relationship, DbAccess access, RelationshipScanCursor cursor) {
        return VirtualValues.node(relationship.endNodeId(consumer(access, cursor)));
    }

    @CalledFromGeneratedCode
    public static VirtualNodeValue otherNode(
            AnyValue anyValue, DbAccess access, VirtualNodeValue node, RelationshipScanCursor cursor) {
        // This is not a function exposed to the user
        assert anyValue != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if (anyValue instanceof VirtualRelationshipValue rel) {
            return otherNode(rel, access, node, cursor);
        } else {
            if (anyValue instanceof Value v)
                throw CypherTypeException.expectedRelValue(
                        String.valueOf(v), v.prettyPrint(), CypherTypeValueMapper.valueType(anyValue));
            else
                throw CypherTypeException.expectedRelValue(
                        String.valueOf(anyValue), String.valueOf(anyValue), CypherTypeValueMapper.valueType(anyValue));
        }
    }

    public static VirtualNodeValue otherNode(
            VirtualRelationshipValue relationship,
            DbAccess access,
            VirtualNodeValue node,
            RelationshipScanCursor cursor) {
        return VirtualValues.node(relationship.otherNodeId(node.id(), consumer(access, cursor)));
    }

    @CalledFromGeneratedCode
    public static AnyValue propertyGet(
            String key,
            AnyValue container,
            DbAccess dbAccess,
            NodeCursor nodeCursor,
            RelationshipScanCursor relationshipScanCursor,
            PropertyCursor propertyCursor) {
        if (container == NO_VALUE) {
            return NO_VALUE;
        } else if (container instanceof VirtualNodeValue node) {
            return dbAccess.nodeProperty(node.id(), dbAccess.propertyKey(key), nodeCursor, propertyCursor, true);
        } else if (container instanceof VirtualRelationshipValue rel) {
            return dbAccess.relationshipProperty(
                    rel, dbAccess.propertyKey(key), relationshipScanCursor, propertyCursor, true);
        } else if (container instanceof MapValue map) {
            return map.get(key);
        } else if (container instanceof TemporalValue<?, ?> temporal) {
            return temporal.get(key);
        } else if (container instanceof DurationValue duration) {
            return duration.get(key);
        } else if (container instanceof PointValue point) {
            return point.get(key);
        } else {
            if (container instanceof Value value)
                throw CypherTypeException.expectedMap(
                        String.valueOf(value), value.prettyPrint(), CypherTypeValueMapper.valueType(container));
            else
                throw CypherTypeException.expectedMap(
                        String.valueOf(container),
                        String.valueOf(container),
                        CypherTypeValueMapper.valueType(container));
        }
    }

    @CalledFromGeneratedCode
    public static AnyValue[] propertiesGet(
            String[] keys,
            AnyValue container,
            DbAccess dbAccess,
            NodeCursor nodeCursor,
            RelationshipScanCursor relationshipScanCursor,
            PropertyCursor propertyCursor) {
        if (container instanceof VirtualNodeValue node) {
            return dbAccess.nodeProperties(node.id(), propertyKeys(keys, dbAccess), nodeCursor, propertyCursor);
        } else if (container instanceof VirtualRelationshipValue rel) {
            return dbAccess.relationshipProperties(
                    rel, propertyKeys(keys, dbAccess), relationshipScanCursor, propertyCursor);
        } else {
            return CursorUtils.propertiesGet(keys, container);
        }
    }

    @CalledFromGeneratedCode
    public static AnyValue containerIndex(
            AnyValue container,
            AnyValue index,
            DbAccess dbAccess,
            NodeCursor nodeCursor,
            RelationshipScanCursor relationshipScanCursor,
            PropertyCursor propertyCursor) {
        if (container == NO_VALUE || index == NO_VALUE) {
            return NO_VALUE;
        } else if (container instanceof VirtualNodeValue node) {
            return dbAccess.nodeProperty(node.id(), propertyKeyId(dbAccess, index), nodeCursor, propertyCursor, true);
        } else if (container instanceof VirtualRelationshipValue rel) {
            return dbAccess.relationshipProperty(
                    rel, propertyKeyId(dbAccess, index), relationshipScanCursor, propertyCursor, true);
        }
        if (container instanceof MapValue map) {
            return mapAccess(map, index);
        } else if (container instanceof SequenceValue seq) {
            return listAccess(seq, index);
        } else {
            if (container instanceof Value v)
                throw CypherTypeException.notCollectionOrMap(
                        String.valueOf(v), v.prettyPrint(), CypherTypeValueMapper.valueType(v), index);
            else
                throw CypherTypeException.notCollectionOrMap(
                        String.valueOf(container),
                        String.valueOf(container),
                        CypherTypeValueMapper.valueType(container),
                        index);
        }
    }

    @CalledFromGeneratedCode
    public static Value containerIndexExists(
            AnyValue container,
            AnyValue index,
            DbAccess dbAccess,
            NodeCursor nodeCursor,
            RelationshipScanCursor relationshipScanCursor,
            PropertyCursor propertyCursor) {
        if (container == NO_VALUE || index == NO_VALUE) {
            return NO_VALUE;
        } else if (container instanceof VirtualNodeValue node) {
            return booleanValue(
                    dbAccess.nodeHasProperty(node.id(), propertyKeyId(dbAccess, index), nodeCursor, propertyCursor));
        } else if (container instanceof VirtualRelationshipValue rel) {
            return booleanValue(dbAccess.relationshipHasProperty(
                    rel, propertyKeyId(dbAccess, index), relationshipScanCursor, propertyCursor));
        }
        if (container instanceof MapValue map) {
            return booleanValue(map.containsKey(asString(
                    index,
                    () ->
                            // this string assumes that the asString method fails and gives context which
                            // operation went wrong
                            "Cannot use non string value as or in map keys. It was " + index.toString())));
        } else {
            if (container instanceof Value v)
                throw CypherTypeException.notMap(
                        String.valueOf(v), v.prettyPrint(), CypherTypeValueMapper.valueType(v), index);
            else
                throw CypherTypeException.notMap(
                        String.valueOf(container),
                        String.valueOf(container),
                        CypherTypeValueMapper.valueType(container),
                        index);
        }
    }

    @CalledFromGeneratedCode
    public static AnyValue head(AnyValue container) {
        if (container == NO_VALUE) {
            return NO_VALUE;
        } else if (container instanceof SequenceValue sequence) {
            if (sequence.intSize() == 0) {
                return NO_VALUE;
            }

            return sequence.value(0);
        } else {
            throw new CypherTypeException(
                    format("Invalid input for function 'head()': Expected %s to be a list", container));
        }
    }

    @CalledFromGeneratedCode
    public static AnyValue tail(AnyValue container) {
        if (container == NO_VALUE) {
            return NO_VALUE;
        } else if (container instanceof ListValue) {
            return ((ListValue) container).tail();
        } else if (container instanceof ArrayValue) {
            return VirtualValues.fromArray((ArrayValue) container).tail();
        } else {
            return EMPTY_LIST;
        }
    }

    @CalledFromGeneratedCode
    public static AnyValue last(AnyValue container) {
        if (container == NO_VALUE) {
            return NO_VALUE;
        }
        if (container instanceof SequenceValue sequence) {
            int length = sequence.intSize();
            if (length == 0) {
                return NO_VALUE;
            }

            return sequence.value(length - 1);
        } else {
            throw new CypherTypeException(
                    format("Invalid input for function 'last()': Expected %s to be a list", container));
        }
    }

    public static AnyValue left(AnyValue in, AnyValue endPos) {
        if (in == NO_VALUE || endPos == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof TextValue text) {
            final long len = asLong(endPos, () -> "Invalid input for length value in function 'left()'");
            return text.substring(0, (int) Math.min(len, Integer.MAX_VALUE));
        } else {
            throw notAString("left", in);
        }
    }

    public static AnyValue ltrim(AnyValue trimSource) {
        if (trimSource == NO_VALUE) {
            return NO_VALUE;
        } else if (trimSource instanceof TextValue) {
            return ((TextValue) trimSource).ltrim();
        } else {
            throw notAString("ltrim", trimSource);
        }
    }

    public static AnyValue ltrim(AnyValue trimSource, AnyValue trimCharacterString) {
        if (trimSource == NO_VALUE || trimCharacterString == NO_VALUE) {
            return NO_VALUE;
        } else if (trimSource instanceof TextValue trimSourceText) {
            if (trimCharacterString instanceof TextValue trimCharacterStringText) {
                return trimSourceText.ltrim(trimCharacterStringText);
            } else {
                throw notAString("ltrim", trimCharacterString);
            }
        } else {
            throw notAString("ltrim", trimSource);
        }
    }

    public static AnyValue rtrim(AnyValue trimSource) {
        if (trimSource == NO_VALUE) {
            return NO_VALUE;
        } else if (trimSource instanceof TextValue) {
            return ((TextValue) trimSource).rtrim();
        } else {
            throw notAString("rtrim", trimSource);
        }
    }

    public static AnyValue rtrim(AnyValue trimSource, AnyValue trimCharacterString) {
        if (trimSource == NO_VALUE || trimCharacterString == NO_VALUE) {
            return NO_VALUE;
        } else if (trimSource instanceof TextValue trimSourceText) {
            if (trimCharacterString instanceof TextValue trimCharacterStringText) {
                return trimSourceText.rtrim(trimCharacterStringText);
            } else {
                throw notAString("rtrim", trimCharacterString);
            }
        } else {
            throw notAString("rtrim", trimSource);
        }
    }

    public static AnyValue btrim(AnyValue trimSource) {
        if (trimSource == NO_VALUE) {
            return NO_VALUE;
        } else if (trimSource instanceof TextValue) {
            return ((TextValue) trimSource).trim();
        } else {
            throw notAString("btrim", trimSource);
        }
    }

    public static AnyValue btrim(AnyValue trimSource, AnyValue trimCharacterString) {
        if (trimSource == NO_VALUE || trimCharacterString == NO_VALUE) {
            return NO_VALUE;
        } else if (trimSource instanceof TextValue trimSourceText) {
            if (trimCharacterString instanceof TextValue trimCharacterStringText) {
                return trimSourceText.trim(trimCharacterStringText);
            } else {
                throw notAString("btrim", trimCharacterString);
            }
        } else {
            throw notAString("btrim", trimSource);
        }
    }

    public static AnyValue trim(AnyValue trimSpecification, AnyValue trimSource) {
        if (trimSource == NO_VALUE) {
            return NO_VALUE;
        }

        if (!(trimSource instanceof TextValue)) {
            throw notAString("trim", trimSource);
        }

        if (trimSpecification instanceof TextValue trimSpec) {
            return switch (trimSpec.stringValue()) {
                case "LEADING" -> ltrim(trimSource);
                case "TRAILING" -> rtrim(trimSource);
                default -> btrim(trimSource);
            };
        } else {
            throw notAString("trim", trimSpecification);
        }
    }

    public static AnyValue trim(AnyValue trimSpecification, AnyValue trimSource, AnyValue trimCharacterString) {
        if (trimSource == NO_VALUE) {
            return NO_VALUE;
        }

        if (!(trimSource instanceof TextValue)) {
            throw notAString("trim", trimSource);
        }

        if (trimSpecification instanceof TextValue trimSpec) {
            if (trimCharacterString instanceof TextValue trimCharString) {
                if (trimCharString.length() != 1) {
                    throw new InvalidArgumentException(
                            "The argument `trimCharacterString` in the `trim()` function must be of length 1.");
                }
            }
            return switch (trimSpec.stringValue()) {
                case "LEADING" -> ltrim(trimSource, trimCharacterString);
                case "TRAILING" -> rtrim(trimSource, trimCharacterString);
                default -> btrim(trimSource, trimCharacterString);
            };
        } else {
            throw notAString("trim", trimSpecification);
        }
    }

    public static AnyValue replace(AnyValue original, AnyValue search, AnyValue replaceWith) {
        if (original == NO_VALUE || search == NO_VALUE || replaceWith == NO_VALUE) {
            return NO_VALUE;
        } else if (original instanceof TextValue) {
            return ((TextValue) original).replace(asString(search), asString(replaceWith));
        } else {
            throw notAString("replace", original);
        }
    }

    public static AnyValue reverse(AnyValue original) {
        if (original == NO_VALUE) {
            return NO_VALUE;
        } else if (original instanceof TextValue text) {
            return text.reverse();
        } else if (original instanceof ListValue list) {
            return list.reverse();
        } else {
            throw new CypherTypeException(
                    "Invalid input for function 'reverse()': "
                            + "Expected a string or a list; consider converting the value to a string with toString() or creating a list.");
        }
    }

    public static AnyValue right(AnyValue original, AnyValue length) {
        if (original == NO_VALUE || length == NO_VALUE) {
            return NO_VALUE;
        } else if (original instanceof TextValue asText) {
            final long len = asLong(length, () -> "Invalid input for length value in function 'right()'");
            if (len < 0) {
                throw new IndexOutOfBoundsException("negative length");
            }
            final long startVal = asText.length() - len;
            return asText.substring((int) Math.max(0, startVal));
        } else {
            throw notAString("right", original);
        }
    }

    public static AnyValue concatenate(AnyValue lhs, AnyValue rhs) {
        if (lhs == NO_VALUE && rhs == NO_VALUE) {
            return NO_VALUE;
        }
        // List Concatenation - Can only concatenate lists when both sides are lists
        // arrays are same as lists when it comes to concatenation
        if (lhs instanceof ArrayValue array) {
            lhs = VirtualValues.fromArray(array);
        }
        if (rhs instanceof ArrayValue array) {
            rhs = VirtualValues.fromArray(array);
        }

        // Null values only return null if the other side is either null (checked already) or a valid concatenation
        // type.
        if (lhs == NO_VALUE) {
            if (rhs instanceof ListValue || rhs instanceof TextValue) {
                return NO_VALUE;
            }
        }

        if (rhs == NO_VALUE) {
            if (lhs instanceof ListValue || lhs instanceof TextValue) {
                return NO_VALUE;
            }
        }

        boolean lhsIsListValue = lhs instanceof ListValue;
        if (lhsIsListValue && rhs instanceof ListValue) {
            return ((ListValue) lhs).appendAll((ListValue) rhs);
        }

        // String Concatenation - Can only concatenate strings when both sides are strings
        if (lhs instanceof TextValue && rhs instanceof TextValue) {
            return ((TextValue) lhs).plus((TextValue) rhs);
        }

        throw new CypherTypeException(
                String.format("Cannot concatenate `%s` and `%s`", lhs.getTypeName(), rhs.getTypeName()));
    }

    public static AnyValue normalize(AnyValue input) {
        return normalize(input, Values.stringValue("NFC"));
    }

    public static AnyValue normalize(AnyValue input, AnyValue normalForm) {
        if (input == NO_VALUE || normalForm == NO_VALUE) {
            return NO_VALUE;
        }

        Normalizer.Form form;
        try {
            form = Normalizer.Form.valueOf(asTextValue(normalForm).stringValue());
        } catch (IllegalArgumentException e) {
            throw InvalidArgumentException.unknownNormalForm(String.valueOf(normalForm));
        }

        String normalized = Normalizer.normalize(asTextValue(input).stringValue(), form);
        return Values.stringValue(normalized);
    }

    public static AnyValue split(AnyValue original, AnyValue separator) {
        if (original == NO_VALUE || separator == NO_VALUE) {
            return NO_VALUE;
        } else if (original instanceof TextValue asText) {
            if (asText.length() == 0) {
                return VirtualValues.list(EMPTY_STRING);
            }
            if (separator instanceof SequenceValue separatorList) {
                var separators = new ArrayList<String>();
                for (var s : separatorList) {
                    if (s == NO_VALUE) {
                        return NO_VALUE;
                    }
                    separators.add(asString(s));
                }
                return asText.split(separators);
            } else {
                return asText.split(asString(separator));
            }
        } else {
            throw notAString("split", original);
        }
    }

    public static AnyValue substring(AnyValue original, AnyValue start) {
        if (original == NO_VALUE || start == NO_VALUE) {
            return NO_VALUE;
        } else if (original instanceof TextValue asText) {

            return asText.substring(asIntExact(start, () -> "Invalid input for start value in function 'substring()'"));
        } else {
            throw notAString("substring", original);
        }
    }

    public static AnyValue substring(AnyValue original, AnyValue start, AnyValue length) {
        if (original == NO_VALUE || start == NO_VALUE || length == NO_VALUE) {
            return NO_VALUE;
        } else if (original instanceof TextValue asText) {

            return asText.substring(
                    asIntExact(start, () -> "Invalid input for start value in function 'substring()'"),
                    asIntExact(length, () -> "Invalid input for length value in function 'substring()'"));
        } else {
            throw notAString("substring", original);
        }
    }

    public static AnyValue toLower(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof TextValue) {
            return ((TextValue) in).toLower();
        } else {
            throw notAString("toLower", in);
        }
    }

    public static AnyValue toUpper(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof TextValue) {
            return ((TextValue) in).toUpper();
        } else {
            throw notAString("toUpper", in);
        }
    }

    public static Value id(AnyValue item) {
        if (item == NO_VALUE) {
            return NO_VALUE;
        } else if (item instanceof VirtualNodeValue) {
            return longValue(((VirtualNodeValue) item).id());
        } else if (item instanceof VirtualRelationshipValue) {
            return longValue(((VirtualRelationshipValue) item).id());
        } else {
            throw new CypherTypeException(format(
                    "Invalid input for function 'id()': Expected %s to be a node or relationship, but it was `%s`",
                    item, item.getTypeName()));
        }
    }

    public static AnyValue elementId(AnyValue entity, ElementIdMapper idMapper) {
        if (entity == NO_VALUE) {
            return NO_VALUE;
        } else if (entity instanceof NodeValue node) {
            // Needed to get correct ids in certain fabric queries.
            return stringValue(node.elementId());
        } else if (entity instanceof VirtualNodeValue node && node.id() < 0) {
            // Don't format element ids for nodes with negative ids, such as db schema visualization
            return stringValue(String.valueOf(node.id()));
        } else if (entity instanceof VirtualNodeValue node) {
            return stringValue(idMapper.nodeElementId(node.id()));
        } else if (entity instanceof RelationshipValue relationship) {
            // Needed to get correct ids in certain fabric queries.
            return stringValue(relationship.elementId());
        } else if (entity instanceof VirtualRelationshipValue relationship && relationship.id() < 0) {
            // Don't format element ids for relationships with negative ids, such as db schema visualization
            return stringValue(String.valueOf(relationship.id()));
        } else if (entity instanceof VirtualRelationshipValue relationship) {
            return stringValue(idMapper.relationshipElementId(relationship.id()));
        }

        throw new CypherTypeException(format(
                "Invalid input for function 'elementId()': Expected %s to be a node or relationship, but it was `%s`",
                entity, entity.getTypeName()));
    }

    public static AnyValue elementIdToNodeId(AnyValue elementId, ElementIdMapper idMapper) {
        if (elementId == NO_VALUE) {
            return NO_VALUE;
        } else if (elementId instanceof TextValue str) {
            try {
                return longValue(idMapper.nodeId(str.stringValue()));
            } catch (IllegalArgumentException e) {
                return NO_VALUE;
            }
        }
        return NO_VALUE;
    }

    public static AnyValue elementIdToRelationshipId(AnyValue elementId, ElementIdMapper idMapper) {
        if (elementId == NO_VALUE) {
            return NO_VALUE;
        } else if (elementId instanceof TextValue str) {
            try {
                return longValue(idMapper.relationshipId(str.stringValue()));
            } catch (IllegalArgumentException e) {
                return NO_VALUE;
            }
        }
        return NO_VALUE;
    }

    public static AnyValue elementIdListToNodeIdList(AnyValue collection, ElementIdMapper idMapper) {
        if (collection == NO_VALUE) {
            return NO_VALUE;
        } else if (collection instanceof SequenceValue elementIds) {
            var builder = ListValueBuilder.newListBuilder(elementIds.intSize());
            for (var elementId : elementIds) {
                AnyValue value = elementIdToNodeId(elementId, idMapper);
                builder.add(value);
            }
            return builder.build();
        }
        return NO_VALUE;
    }

    public static AnyValue elementIdListToRelationshipIdList(AnyValue collection, ElementIdMapper idMapper) {
        if (collection == NO_VALUE) {
            return NO_VALUE;
        } else if (collection instanceof SequenceValue elementIds) {
            var builder = ListValueBuilder.newListBuilder(elementIds.intSize());
            for (var elementId : elementIds) {
                AnyValue value = elementIdToRelationshipId(elementId, idMapper);
                builder.add(value);
            }
            return builder.build();
        }
        return NO_VALUE;
    }

    public static TextValue nodeElementId(long id, ElementIdMapper idMapper) {
        assert id > LongReference.NULL;
        return Values.stringValue(idMapper.nodeElementId(id));
    }

    public static TextValue relationshipElementId(long id, ElementIdMapper idMapper) {
        assert id > LongReference.NULL;
        return Values.stringValue(idMapper.relationshipElementId(id));
    }

    public static AnyValue labels(AnyValue item, DbAccess access, NodeCursor nodeCursor) {
        if (item == NO_VALUE) {
            return NO_VALUE;
        } else if (item instanceof NodeEntityWrappingNodeValue node && node.id() < 0) {
            // Labels for entities with negative id, such as db schema visualization, are already populated since
            // the entity isn't a node in storage
            var builder = ListValueBuilder.newListBuilder(node.labels().intSize());
            node.labels().forEach(builder::add);
            return builder.build();
        } else if (item instanceof VirtualNodeValue node) {
            return access.getLabelsForNode(node.id(), nodeCursor);
        } else {
            throw new CypherTypeException("Invalid input for function 'labels()': Expected a Node, got: " + item);
        }
    }

    @CalledFromGeneratedCode
    public static boolean hasLabel(AnyValue entity, int labelToken, DbAccess access, NodeCursor nodeCursor) {
        assert entity != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if (entity instanceof VirtualNodeValue node) {
            return access.isLabelSetOnNode(labelToken, node.id(), nodeCursor);
        } else {
            if (entity instanceof Value v)
                throw CypherTypeException.expectedNode(
                        String.valueOf(v), v.prettyPrint(), CypherTypeValueMapper.valueType(v));
            else
                throw CypherTypeException.expectedNode(
                        String.valueOf(entity), String.valueOf(entity), CypherTypeValueMapper.valueType(entity));
        }
    }

    @CalledFromGeneratedCode
    public static boolean hasLabels(AnyValue entity, int[] labelTokens, DbAccess access, NodeCursor nodeCursor) {
        assert entity != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if (entity instanceof VirtualNodeValue node) {
            return access.areLabelsSetOnNode(labelTokens, node.id(), nodeCursor);
        } else {
            if (entity instanceof Value v)
                throw CypherTypeException.expectedNode(
                        String.valueOf(v), v.prettyPrint(), CypherTypeValueMapper.valueType(v));
            else
                throw CypherTypeException.expectedNode(
                        String.valueOf(entity), String.valueOf(entity), CypherTypeValueMapper.valueType(entity));
        }
    }

    private static boolean hasLabel(
            VirtualNodeValue node, TextValue textLabel, NodeCursor nodeCursor, QueryContext queryContext)
            throws IllegalTokenNameException {
        var validName = TokenWrite.checkValidTokenName(textLabel.stringValue());
        var tokenId = queryContext.nodeLabel(validName);
        if (tokenId == TokenConstants.NO_TOKEN) {
            return false;
        }

        return queryContext.isLabelSetOnNode(tokenId, node.id(), nodeCursor);
    }

    public static String evaluateSingleDynamicRelType(AnyValue value) throws IllegalTokenNameException {
        TextValue singleValue = null;

        if (value instanceof TextValue textValue) {
            singleValue = textValue;
        } else if (value instanceof SequenceValue sequenceValue) {
            for (var t : sequenceValue) {
                if (t instanceof TextValue textValue) {
                    if (singleValue == null) {
                        singleValue = textValue;
                    } else if (!singleValue.equals(textValue)) {
                        throw new IllegalArgumentException("Error - Exactly one relationship type must be specified.");
                    }
                } else {
                    throw new CypherTypeException(format(
                            "Invalid input for function 'evaluateDynamicRelType()': Expected %s to be a string, but it was a `%s`",
                            t, t.getTypeName()));
                }
            }
        } else {
            throw new CypherTypeException(format(
                    "Invalid input for function 'evaluateDynamicRelType()': Expected %s to be a string or list of strings, but it was a `%s`",
                    value, value.getTypeName()));
        }
        if (singleValue == null) {
            // can only reach here if value was an empty sequence
            throw new IllegalArgumentException("Error - Exactly one relationship type must be specified.");
        }

        return singleValue.stringValue();
    }

    public static int getOrCreateDynamicRelType(AnyValue value, QueryContext queryContext)
            throws IllegalTokenNameException {
        return queryContext.getOrCreateRelTypeId(evaluateSingleDynamicRelType(value));
    }

    public static int getOrCreateDynamicRelType(AnyValue value, TokenWrite token) throws KernelException {
        return token.relationshipTypeGetOrCreateForName(evaluateSingleDynamicRelType(value));
    }

    @CalledFromGeneratedCode
    public static boolean hasDynamicLabels(
            AnyValue entity, AnyValue[] labelNames, NodeCursor nodeCursor, QueryContext queryContext)
            throws IllegalTokenNameException {
        assert entity != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if (entity instanceof VirtualNodeValue node) {
            for (var labelName : labelNames) {
                if (labelName instanceof TextValue textLabel) {
                    if (!hasLabel(node, textLabel, nodeCursor, queryContext)) {
                        return false;
                    }
                } else if (labelName instanceof SequenceValue labelSequence) {
                    for (var l : labelSequence) {
                        if (l instanceof TextValue textLabel) {
                            if (!hasLabel(node, textLabel, nodeCursor, queryContext)) {
                                return false;
                            }
                        } else {
                            throw new CypherTypeException(format(
                                    "Invalid input for function 'hasDynamicLabels()': Expected %s to be a string, but it was `%s`",
                                    labelName, labelName.getTypeName()));
                        }
                    }
                } else {
                    throw new CypherTypeException(format(
                            "Invalid input for function 'hasDynamicLabels()': Expected %s to be a string or list of strings, but it was `%s`",
                            labelName, labelName.getTypeName()));
                }
            }
        } else {
            throw new CypherTypeException(format(
                    "Invalid input for function 'hasDynamicLabels()': Expected %s to be a node, but it was `%s`",
                    entity, entity.getTypeName()));
        }
        return true;
    }

    @CalledFromGeneratedCode
    public static boolean hasALabel(AnyValue entity, DbAccess access, NodeCursor nodeCursor) {
        assert entity != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if (entity instanceof VirtualNodeValue virtualNodeValue) {
            return access.isALabelSetOnNode(virtualNodeValue.id(), nodeCursor);
        } else {
            if (entity instanceof Value v)
                throw CypherTypeException.expectedNode(
                        String.valueOf(v), v.prettyPrint(), CypherTypeValueMapper.valueType(v));
            else
                throw CypherTypeException.expectedNode(
                        String.valueOf(entity), String.valueOf(entity), CypherTypeValueMapper.valueType(entity));
        }
    }

    @CalledFromGeneratedCode
    public static boolean hasALabelOrType(AnyValue entity, DbAccess access, NodeCursor nodeCursor) {
        assert entity != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if (entity instanceof VirtualNodeValue node) {
            return access.isALabelSetOnNode(node.id(), nodeCursor);
        } else if (entity instanceof VirtualRelationshipValue) {
            return true;
        } else {
            throw new CypherTypeException(format(
                    "Invalid input for function 'hasALabelOrType()': Expected %s to be a node or relationship, but it was `%s`",
                    entity, entity.getTypeName()));
        }
    }

    @CalledFromGeneratedCode
    public static boolean hasLabelsOrTypes(
            AnyValue entity,
            DbAccess access,
            int[] labels,
            NodeCursor nodeCursor,
            int[] types,
            RelationshipScanCursor relationshipScanCursor) {
        assert entity != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if (entity instanceof VirtualNodeValue node) {
            return access.areLabelsSetOnNode(labels, node.id(), nodeCursor);
        } else if (entity instanceof VirtualRelationshipValue relationship) {
            return access.areTypesSetOnRelationship(types, relationship, relationshipScanCursor);
        } else {
            throw new CypherTypeException(format(
                    "Invalid input for function 'hasALabelOrType()': Expected %s to be a node or relationship, but it was `%s`",
                    entity, entity.getTypeName()));
        }
    }

    @CalledFromGeneratedCode
    public static boolean hasAnyLabel(AnyValue entity, int[] labels, DbAccess access, NodeCursor nodeCursor) {
        assert entity != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if (entity instanceof VirtualNodeValue node) {
            return access.isAnyLabelSetOnNode(labels, node.id(), nodeCursor);
        } else {
            if (entity instanceof Value v)
                throw CypherTypeException.expectedNode(
                        String.valueOf(v), v.prettyPrint(), CypherTypeValueMapper.valueType(v));
            else
                throw CypherTypeException.expectedNode(
                        String.valueOf(entity), String.valueOf(entity), CypherTypeValueMapper.valueType(entity));
        }
    }

    @CalledFromGeneratedCode
    public static boolean hasAnyDynamicLabel(
            AnyValue entity, AnyValue[] labels, NodeCursor nodeCursor, QueryContext queryContext)
            throws IllegalTokenNameException {
        assert entity != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if (entity instanceof VirtualNodeValue node) {
            for (var labelName : labels) {
                if (labelName instanceof TextValue textLabel) {
                    if (hasLabel(node, textLabel, nodeCursor, queryContext)) {
                        return true;
                    }
                } else if (labelName instanceof SequenceValue labelSequence) {
                    for (var l : labelSequence) {
                        if (l instanceof TextValue textLabel) {
                            if (hasLabel(node, textLabel, nodeCursor, queryContext)) {
                                return true;
                            }
                        } else {
                            throw new CypherTypeException(format(
                                    "Invalid input for function 'hasAnyDynamicLabel()': Expected %s to be a string, but it was a `%s`",
                                    l, l.getTypeName()));
                        }
                    }
                } else {
                    throw new CypherTypeException(format(
                            "Invalid input for function 'hasAnyDynamicLabel()': Expected %s to be a string or list of strings, but it was a `%s`",
                            labelName, labelName.getTypeName()));
                }
            }
        } else {
            throw new CypherTypeException(format(
                    "Invalid input for function 'hasAnyDynamicLabel()': Expected %s to be a node, but it was a `%s`",
                    entity, entity.getTypeName()));
        }

        return false;
    }

    public static AnyValue type(AnyValue item, DbAccess access, RelationshipScanCursor relCursor, Read read) {
        if (item == NO_VALUE) {
            return NO_VALUE;
        } else if (item instanceof RelationshipValue relationship) {
            return relationship.type();
        } else if (item instanceof VirtualRelationshipValue relationship) {

            int typeToken = relationship.relationshipTypeId(relationshipVisitor -> {
                long relationshipId = relationshipVisitor.id();
                access.singleRelationship(relationshipId, relCursor);

                if (relCursor.next() || read.relationshipDeletedInTransaction(relationshipId)) {
                    relationshipVisitor.visit(
                            relCursor.sourceNodeReference(), relCursor.targetNodeReference(), relCursor.type());
                }
            });

            if (typeToken == TokenConstants.NO_TOKEN) {
                return NO_VALUE;
            } else {
                return Values.stringValue(access.relationshipTypeName(typeToken));
            }
        } else {
            throw new CypherTypeException("Invalid input for function 'type()': Expected a Relationship, got: " + item);
        }
    }

    @CalledFromGeneratedCode
    public static boolean hasType(AnyValue entity, int typeToken, DbAccess access, RelationshipScanCursor relCursor) {
        assert entity != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if (entity instanceof VirtualRelationshipValue relationship) {
            if (typeToken == StatementConstants.NO_SUCH_RELATIONSHIP_TYPE) {
                return false;
            } else {
                return typeToken == relationship.relationshipTypeId(consumer(access, relCursor));
            }
        } else {
            if (entity instanceof Value v)
                throw CypherTypeException.expectedRel(
                        String.valueOf(v), v.prettyPrint(), CypherTypeValueMapper.valueType(v));
            else
                throw CypherTypeException.expectedRel(
                        String.valueOf(entity), String.valueOf(entity), CypherTypeValueMapper.valueType(entity));
        }
    }

    @CalledFromGeneratedCode
    public static boolean hasTypes(
            AnyValue entity, int[] typeTokens, DbAccess access, RelationshipScanCursor relCursor) {
        assert entity != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if (entity instanceof VirtualRelationshipValue relationship) {
            return access.areTypesSetOnRelationship(typeTokens, relationship, relCursor);
        } else {
            if (entity instanceof Value v)
                throw CypherTypeException.expectedRel(
                        String.valueOf(v), v.prettyPrint(), CypherTypeValueMapper.valueType(v));
            else
                throw CypherTypeException.expectedRel(
                        String.valueOf(entity), String.valueOf(entity), CypherTypeValueMapper.valueType(entity));
        }
    }

    private static boolean hasType(
            VirtualRelationshipValue relationship,
            TextValue textValue,
            RelationshipScanCursor relCursor,
            DbAccess queryContext)
            throws IllegalTokenNameException {
        var validName = TokenWrite.checkValidTokenName(textValue.stringValue());
        var tokenId = queryContext.relationshipType(validName);
        return queryContext.isTypeSetOnRelationship(tokenId, relationship.id(), relCursor);
    }

    @CalledFromGeneratedCode
    public static boolean hasDynamicType(
            AnyValue entity,
            AnyValue[] dynamicTypes,
            RelationshipScanCursor relCursor,
            DbAccess queryContext,
            RuntimeNotifier notifier)
            throws IllegalTokenNameException {
        assert entity != NO_VALUE : "NO_VALUE checks need to happen outside this call";

        TextValue singleValue = null;
        ArrayList<String> conflictingTypes = null;
        if (entity instanceof VirtualRelationshipValue relationship) {
            for (var value : dynamicTypes) {
                if (value instanceof TextValue textValue) {
                    if (conflictingTypes != null) {
                        conflictingTypes.add(textValue.stringValue());
                    } else if (singleValue == null) {
                        singleValue = textValue;
                    } else if (!singleValue.equals(textValue)) {
                        conflictingTypes = new ArrayList<>();
                        conflictingTypes.add(singleValue.stringValue());
                        conflictingTypes.add(textValue.stringValue());
                    }
                } else if (value instanceof SequenceValue sequenceValue) {
                    for (var t : sequenceValue) {
                        if (t instanceof TextValue textValue) {
                            if (conflictingTypes != null) {
                                conflictingTypes.add(textValue.stringValue());
                            } else if (singleValue == null) {
                                singleValue = textValue;
                            } else if (!singleValue.equals(textValue)) {
                                conflictingTypes = new ArrayList<>();
                                conflictingTypes.add(singleValue.stringValue());
                                conflictingTypes.add(textValue.stringValue());
                            }
                        } else {
                            throw new CypherTypeException(format(
                                    "Invalid input for function 'hasDynamicType()': Expected %s to be a string, but it was a `%s`",
                                    t, t.getTypeName()));
                        }
                    }
                } else {
                    throw new CypherTypeException(format(
                            "Invalid input for function 'hasDynamicType()': Expected %s to be a string or list of strings, but it was a `%s`",
                            value, value.getTypeName()));
                }
            }

            if (singleValue == null) {
                // weird but if we have no value by this point then the list was empty and all(for n in []) == true
                return true;
            }

            if (conflictingTypes != null) {
                // detected more than one relationship type; the conjunction can never be satisfied
                notifier.newRuntimeNotification(new RuntimeUnsatisfiableRelationshipTypeExpression(conflictingTypes));
                return false;
            }

            return hasType(relationship, singleValue, relCursor, queryContext);
        } else {
            throw new CypherTypeException(format(
                    "Invalid input for function 'hasDynamicType()': Expected %s to be a relationship, but it was a `%s`",
                    entity, entity.getTypeName()));
        }
    }

    @CalledFromGeneratedCode
    public static boolean hasAnyDynamicType(
            AnyValue entity, AnyValue[] dynamicTypes, RelationshipScanCursor relCursor, QueryContext queryContext)
            throws IllegalTokenNameException {
        assert entity != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if (entity instanceof VirtualRelationshipValue relationship) {
            for (var typ : dynamicTypes) {
                if (typ instanceof TextValue textValue) {
                    if (hasType(relationship, textValue, relCursor, queryContext)) {
                        return true;
                    }
                } else if (typ instanceof SequenceValue typeSeq) {
                    for (var t : typeSeq) {
                        if (t instanceof TextValue textValue) {
                            if (hasType(relationship, textValue, relCursor, queryContext)) {
                                return true;
                            }
                        } else {
                            throw new CypherTypeException(format(
                                    "Invalid input for function 'hasAnyDynamicType()': Expected %s to be a string, but it was a `%s`",
                                    t, t.getTypeName()));
                        }
                    }
                } else {
                    throw new CypherTypeException(format(
                            "Invalid input for function 'hasAnyDynamicType()': Expected %s to be a string or list of strings, but it was a `%s`",
                            typ, typ.getTypeName()));
                }
            }
        } else {
            throw new CypherTypeException(format(
                    "Invalid input for function 'hasAnyDynamicType()': Expected %s to be a relationship, but it was a `%s`",
                    entity, entity.getTypeName()));
        }

        return false;
    }

    public static AnyValue nodes(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof PathValue) {
            return VirtualValues.list(((PathValue) in).nodes());
        } else if (in instanceof VirtualPathValue) {
            long[] ids = ((VirtualPathValue) in).nodeIds();
            ListValueBuilder builder = ListValueBuilder.newListBuilder(ids.length);
            for (long id : ids) {
                builder.add(VirtualValues.node(id));
            }
            return builder.build();
        } else {
            throw new CypherTypeException(format("Invalid input for function 'nodes()': Expected %s to be a path", in));
        }
    }

    public static AnyValue relationships(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof VirtualPathValue path) {
            return path.relationshipsAsList();
        } else {
            throw new CypherTypeException(
                    format("Invalid input for function 'relationships()': Expected %s to be a path", in));
        }
    }

    public static Value point(AnyValue in, DbAccess access, ExpressionCursors cursors) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof VirtualNodeValue node) {
            return asPoint(access, node, cursors.nodeCursor(), cursors.propertyCursor());
        } else if (in instanceof VirtualRelationshipValue rel) {
            return asPoint(access, rel, cursors.relationshipScanCursor(), cursors.propertyCursor());
        } else if (in instanceof MapValue map) {
            if (containsNull(map)) {
                return NO_VALUE;
            }
            return PointValue.fromMap(map);
        } else {
            throw new CypherTypeException(
                    format("Invalid input for function 'point()': Expected a map but got %s", in));
        }
    }

    public static AnyValue keys(
            AnyValue in,
            DbAccess access,
            NodeCursor nodeCursor,
            RelationshipScanCursor relationshipScanCursor,
            PropertyCursor propertyCursor) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof VirtualNodeValue node) {
            return extractKeys(access, access.nodePropertyIds(node.id(), nodeCursor, propertyCursor));
        } else if (in instanceof VirtualRelationshipValue rel) {
            return extractKeys(access, access.relationshipPropertyIds(rel, relationshipScanCursor, propertyCursor));
        } else if (in instanceof MapValue) {
            return ((MapValue) in).keys();
        } else {
            throw new CypherTypeException(format(
                    "Invalid input for function 'keys()': Expected a node, a relationship or a literal map but got %s",
                    in));
        }
    }

    public static AnyValue properties(
            AnyValue in,
            DbAccess access,
            NodeCursor nodeCursor,
            RelationshipScanCursor relationshipCursor,
            PropertyCursor propertyCursor) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof VirtualNodeValue node) {
            return access.nodeAsMap(
                    node.id(), nodeCursor, propertyCursor, new MapValueBuilder(), IntSets.immutable.empty());
        } else if (in instanceof VirtualRelationshipValue rel) {
            return access.relationshipAsMap(
                    rel, relationshipCursor, propertyCursor, new MapValueBuilder(), IntSets.immutable.empty());
        } else if (in instanceof MapValue) {
            return in;
        } else {
            throw new CypherTypeException(format(
                    "Invalid input for function 'properties()': Expected a node, a relationship or a literal map but got %s",
                    in));
        }
    }

    public static AnyValue properties(
            AnyValue in,
            DbAccess access,
            NodeCursor nodeCursor,
            RelationshipScanCursor relationshipCursor,
            PropertyCursor propertyCursor,
            MapValueBuilder alreadyReadProperties,
            IntSet alreadyReadPropertyTokens) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof VirtualNodeValue node) {
            return access.nodeAsMap(
                    node.id(), nodeCursor, propertyCursor, alreadyReadProperties, alreadyReadPropertyTokens);
        } else if (in instanceof VirtualRelationshipValue rel) {
            return access.relationshipAsMap(
                    rel.id(), relationshipCursor, propertyCursor, alreadyReadProperties, alreadyReadPropertyTokens);
        } else if (in instanceof MapValue) {
            return in;
        } else {
            throw new CypherTypeException(format(
                    "Invalid input for function 'properties()': Expected a node, a relationship or a literal map but got %s",
                    in));
        }
    }

    public static AnyValue characterLength(AnyValue item) {
        if (item == NO_VALUE) {
            return NO_VALUE;
        } else if (item instanceof TextValue) {
            return size(item);
        } else {
            throw new CypherTypeException(
                    "Invalid input for function 'character_length()': Expected a String, got: " + item);
        }
    }

    public static AnyValue size(AnyValue item) {
        if (item == NO_VALUE) {
            return NO_VALUE;
        } else if (item instanceof TextValue) {
            return longValue(((TextValue) item).length());
        } else if (item instanceof SequenceValue) {
            return longValue(((SequenceValue) item).actualSize());
        } else {
            throw new CypherTypeException(
                    "Invalid input for function 'size()': Expected a String or List, got: " + item);
        }
    }

    public static AnyValue isEmpty(AnyValue item) {
        if (item == NO_VALUE) {
            return NO_VALUE;
        } else if (item instanceof SequenceValue) {
            return Values.booleanValue(((SequenceValue) item).isEmpty());
        } else if (item instanceof MapValue) {
            return Values.booleanValue(((MapValue) item).isEmpty());
        } else if (item instanceof TextValue) {
            return Values.booleanValue(((TextValue) item).isEmpty());
        } else {
            throw new CypherTypeException(
                    "Invalid input for function 'isEmpty()': Expected a List, Map, or String, got: " + item);
        }
    }

    public static AnyValue length(AnyValue item) {
        if (item == NO_VALUE) {
            return NO_VALUE;
        } else if (item instanceof VirtualPathValue) {
            return longValue(((VirtualPathValue) item).size());
        } else {
            throw new CypherTypeException("Invalid input for function 'length()': Expected a Path, got: " + item);
        }
    }

    public static Value toBoolean(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof BooleanValue) {
            return (BooleanValue) in;
        } else if (in instanceof TextValue) {
            return switch (((TextValue) in).trim().stringValue().toLowerCase(Locale.ROOT)) {
                case "true" -> TRUE;
                case "false" -> FALSE;
                default -> NO_VALUE;
            };
        } else if (in instanceof IntegralValue integer) {
            return integer.longValue() == 0L ? FALSE : TRUE;
        } else {
            throw new CypherTypeException(
                    "Invalid input for function 'toBoolean()': Expected a Boolean, Integer or String, got: " + in);
        }
    }

    public static Value toBooleanOrNull(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof BooleanValue || in instanceof TextValue || in instanceof IntegralValue) {
            return toBoolean(in);
        } else {
            return NO_VALUE;
        }
    }

    public static AnyValue toBooleanList(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof SequenceValue sv) {
            return StreamSupport.stream(sv.spliterator(), false)
                    .map(entry -> entry == NO_VALUE ? NO_VALUE : toBooleanOrNull(entry))
                    .collect(ListValueBuilder.collector());
        } else {
            throw new CypherTypeException(
                    String.format("Invalid input for function 'toBooleanList()': Expected a List, got: %s", in));
        }
    }

    public static Value toFloat(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof DoubleValue) {
            return (DoubleValue) in;
        } else if (in instanceof NumberValue number) {
            return doubleValue(number.doubleValue());
        } else if (in instanceof TextValue) {
            try {
                return doubleValue(parseDouble(((TextValue) in).stringValue()));
            } catch (NumberFormatException ignore) {
                return NO_VALUE;
            }
        } else {
            throw new CypherTypeException(
                    "Invalid input for function 'toFloat()': Expected a String, Float or Integer, got: " + in);
        }
    }

    public static Value toFloatOrNull(AnyValue in) {
        if (in instanceof NumberValue || in instanceof TextValue) {
            return toFloat(in);
        } else {
            return NO_VALUE;
        }
    }

    public static AnyValue toFloatList(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof SequenceValue sv) {
            return StreamSupport.stream(sv.spliterator(), false)
                    .map(entry -> entry == NO_VALUE ? NO_VALUE : toFloatOrNull(entry))
                    .collect(ListValueBuilder.collector());
        } else {
            throw new CypherTypeException(
                    String.format("Invalid input for function 'toFloatList()': Expected a List, got: %s", in));
        }
    }

    public static Value toInteger(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof IntegralValue) {
            return (IntegralValue) in;
        } else if (in instanceof NumberValue number) {
            return longValue(number.longValue());
        } else if (in instanceof TextValue) {
            return stringToLongValue((TextValue) in);
        } else if (in instanceof BooleanValue) {
            if (((BooleanValue) in).booleanValue()) {
                return longValue(1L);
            } else {
                return longValue(0L);
            }
        } else {
            throw new CypherTypeException(
                    "Invalid input for function 'toInteger()': Expected a String, Float, Integer or Boolean, got: "
                            + in);
        }
    }

    public static Value toIntegerOrNull(AnyValue in) {
        if (in instanceof NumberValue || in instanceof BooleanValue) {
            return toInteger(in);
        } else if (in instanceof TextValue) {
            try {
                return stringToLongValue((TextValue) in);
            } catch (CypherTypeException e) {
                return NO_VALUE;
            }
        } else {
            return NO_VALUE;
        }
    }

    public static AnyValue toIntegerList(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof IntegralArray array) {
            return VirtualValues.fromArray(array);
        } else if (in instanceof FloatingPointArray array) {
            return toIntegerList(array);
        } else if (in instanceof SequenceValue sequence) {
            return toIntegerList(sequence);
        } else {
            throw new CypherTypeException(
                    String.format("Invalid input for function 'toIntegerList()': Expected a List, got: %s", in));
        }
    }

    public static Value toString(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof TextValue text) {
            return text;
        } else if (in instanceof NumberValue number) {
            return stringValue(number.prettyPrint());
        } else if (in instanceof BooleanValue b) {
            return stringValue(b.prettyPrint());
        } else if (in instanceof TemporalValue || in instanceof DurationValue || in instanceof PointValue) {
            return stringValue(in.toString());
        } else {
            throw new CypherTypeException(
                    "Invalid input for function 'toString()': Expected a String, Float, Integer, Boolean, Temporal or Duration, got: "
                            + in);
        }
    }

    public static AnyValue toStringOrNull(AnyValue in) {
        if (in instanceof TextValue
                || in instanceof NumberValue
                || in instanceof BooleanValue
                || in instanceof TemporalValue
                || in instanceof DurationValue
                || in instanceof PointValue) {
            return toString(in);
        } else {
            return NO_VALUE;
        }
    }

    public static AnyValue toStringList(AnyValue in) {
        if (in == NO_VALUE) {
            return NO_VALUE;
        } else if (in instanceof SequenceValue sv) {
            return StreamSupport.stream(sv.spliterator(), false)
                    .map(entry -> entry == NO_VALUE ? NO_VALUE : toStringOrNull(entry))
                    .collect(ListValueBuilder.collector());
        } else {
            throw new CypherTypeException(
                    String.format("Invalid input for function 'toStringList()': Expected a List, got: %s", in));
        }
    }

    public static AnyValue fromSlice(AnyValue collection, AnyValue fromValue) {
        if (collection == NO_VALUE || fromValue == NO_VALUE) {
            return NO_VALUE;
        }

        int from = asIntExact(fromValue);
        ListValue list = asList(collection);
        if (from >= 0) {
            return list.drop(from);
        } else {
            return list.drop(list.actualSize() + from);
        }
    }

    public static AnyValue toSlice(AnyValue collection, AnyValue toValue) {
        if (collection == NO_VALUE || toValue == NO_VALUE) {
            return NO_VALUE;
        }
        int from = asIntExact(toValue);
        ListValue list = asList(collection);
        if (from >= 0) {
            return list.take(from);
        } else {
            return list.take(list.intSize() + from);
        }
    }

    public static AnyValue fullSlice(AnyValue collection, AnyValue fromValue, AnyValue toValue) {
        if (collection == NO_VALUE || fromValue == NO_VALUE || toValue == NO_VALUE) {
            return NO_VALUE;
        }
        int from = asIntExact(fromValue);
        int to = asIntExact(toValue);
        ListValue list = asList(collection);
        int size = list.intSize();
        if (from >= 0 && to >= 0) {
            return list.slice(from, to);
        } else if (from >= 0) {
            return list.slice(from, size + to);
        } else if (to >= 0) {
            return list.slice(size + from, to);
        } else {
            return list.slice(size + from, size + to);
        }
    }

    @CalledFromGeneratedCode
    public static TextValue asTextValue(AnyValue value) {
        return asTextValue(value, null);
    }

    public static TextValue asTextValue(AnyValue value, Supplier<String> contextForErrorMessage) {
        if (!(value instanceof TextValue)) {
            String errorMessage;
            if (contextForErrorMessage == null) {
                errorMessage = format(
                        "Expected %s to be a %s, but it was a %s",
                        value, TextValue.class.getName(), value.getClass().getName());
            } else {
                errorMessage = format(
                        "%s: Expected %s to be a %s, but it was a %s",
                        contextForErrorMessage.get(),
                        value,
                        TextValue.class.getName(),
                        value.getClass().getName());
            }
            if (value instanceof Value v)
                throw CypherTypeException.expectedString(
                        errorMessage, v.prettyPrint(), CypherTypeValueMapper.valueType(v));
            else
                throw CypherTypeException.expectedString(
                        errorMessage, String.valueOf(value), CypherTypeValueMapper.valueType(value));
        }
        return (TextValue) value;
    }

    private static Value stringToLongValue(TextValue in) {
        try {
            return longValue(parseLong(in.stringValue()));
        } catch (Exception e) {
            try {
                BigDecimal bigDecimal = new BigDecimal(in.stringValue());
                if (bigDecimal.compareTo(MAX_LONG) <= 0 && bigDecimal.compareTo(MIN_LONG) >= 0) {
                    return longValue(bigDecimal.longValue());
                } else {
                    throw new CypherTypeException(format("integer, %s, is too large", in.stringValue()));
                }
            } catch (NumberFormatException ignore) {
                return NO_VALUE;
            }
        }
    }

    private static ListValue extractKeys(DbAccess access, int[] keyIds) {
        String[] keysNames = new String[keyIds.length];
        for (int i = 0; i < keyIds.length; i++) {
            keysNames[i] = access.getPropertyKeyName(keyIds[i]);
        }
        return VirtualValues.fromArray(Values.stringArray(keysNames));
    }

    private static Value asPoint(
            DbAccess access, VirtualNodeValue nodeValue, NodeCursor nodeCursor, PropertyCursor propertyCursor) {
        MapValueBuilder builder = new MapValueBuilder();
        for (String key : POINT_KEYS) {
            Value value =
                    access.nodeProperty(nodeValue.id(), access.propertyKey(key), nodeCursor, propertyCursor, true);
            if (value == NO_VALUE) {
                continue;
            }
            builder.add(key, value);
        }

        return PointValue.fromMap(builder.build());
    }

    private static Value asPoint(
            DbAccess access,
            VirtualRelationshipValue relationshipValue,
            RelationshipScanCursor relationshipScanCursor,
            PropertyCursor propertyCursor) {
        MapValueBuilder builder = new MapValueBuilder();
        for (String key : POINT_KEYS) {
            Value value = access.relationshipProperty(
                    relationshipValue, access.propertyKey(key), relationshipScanCursor, propertyCursor, true);
            if (value == NO_VALUE) {
                continue;
            }
            builder.add(key, value);
        }

        return PointValue.fromMap(builder.build());
    }

    private static boolean containsNull(MapValue map) {
        boolean[] hasNull = {false};
        map.foreach((s, value) -> {
            if (value == NO_VALUE) {
                hasNull[0] = true;
            }
        });
        return hasNull[0];
    }

    private static AnyValue listAccess(SequenceValue container, AnyValue index) {
        NumberValue number = asNumberValue(
                index,
                () -> "Cannot access a list '" + container.toString() + "' using a non-number index, got "
                        + index.toString());
        if (!(number instanceof IntegralValue)) {
            throw CypherTypeException.nonIntegerListIndex(
                    String.valueOf(number), number.prettyPrint(), CypherTypeValueMapper.valueType(number));
        }
        long idx = number.longValue();

        if (idx < 0) {
            idx = container.actualSize() + idx;
        }
        if (idx >= container.actualSize() || idx < 0) {
            return NO_VALUE;
        }
        return container.value(idx);
    }

    private static int propertyKeyId(DbAccess dbAccess, AnyValue index) {
        return dbAccess.propertyKey(asString(
                index,
                () ->
                        // this string assumes that the asString method fails and gives context which operation went
                        // wrong
                        "Cannot use a property key with non string name. It was " + index.toString()));
    }

    private static AnyValue mapAccess(MapValue container, AnyValue index) {
        return container.get(asString(
                index,
                () ->
                        // this string assumes that the asString method fails and gives context which operation went
                        // wrong
                        "Cannot access a map '" + container.toString() + "' by key '" + index.toString() + "'"));
    }

    public static String asString(AnyValue value) {
        return asTextValue(value).stringValue();
    }

    public static List<String> asStringList(AnyValue value) {
        if (value instanceof TextValue text) {
            return Collections.singletonList(text.stringValue());
        } else if (value instanceof SequenceValue sequenceValue) {
            List<String> result = new ArrayList<>();
            sequenceValue.forEach(t -> result.add(asTextValue(t).stringValue()));
            return result;
        } else {
            throw new CypherTypeException(String.format(
                    "Expected %s to be a %s or a %s, but it was a %s",
                    value, TextValue.class.getName(), SequenceValue.class.getName(), value.getTypeName()));
        }
    }

    private static String asString(AnyValue value, Supplier<String> contextForErrorMessage) {
        return asTextValue(value, contextForErrorMessage).stringValue();
    }

    private static NumberValue asNumberValue(AnyValue value, Supplier<String> contextForErrorMessage) {
        if (!(value instanceof NumberValue)) {
            var msg = format(
                    "%s: Expected %s to be a %s, but it was a %s",
                    contextForErrorMessage.get(),
                    value,
                    NumberValue.class.getName(),
                    value.getClass().getName());
            if (value instanceof Value v)
                throw CypherTypeException.expectedNumber(msg, v.prettyPrint(), CypherTypeValueMapper.valueType(v));
            else
                throw CypherTypeException.expectedNumber(
                        msg, String.valueOf(value), CypherTypeValueMapper.valueType(value));
        }
        return (NumberValue) value;
    }

    private static Value calculateDistance(PointValue p1, PointValue p2) {
        if (p1.getCoordinateReferenceSystem().equals(p2.getCoordinateReferenceSystem())) {
            return doubleValue(p1.getCoordinateReferenceSystem().getCalculator().distance(p1, p2));
        } else {
            return NO_VALUE;
        }
    }

    private static long asLong(AnyValue value, Supplier<String> contextForErrorMessage) {
        if (value instanceof NumberValue) {
            return ((NumberValue) value).longValue();
        } else {
            String errorMsg;
            if (contextForErrorMessage == null) {
                errorMsg = "Expected a numeric value but got: " + value;
            } else {
                errorMsg = contextForErrorMessage.get() + ": Expected a numeric value but got: " + value;
            }
            if (value instanceof Value v)
                throw CypherTypeException.expectedNumber(errorMsg, v.prettyPrint(), CypherTypeValueMapper.valueType(v));
            else
                throw CypherTypeException.expectedNumber(
                        errorMsg, String.valueOf(value), CypherTypeValueMapper.valueType(value));
        }
    }

    public static int asIntExact(AnyValue value) {
        return asIntExact(value, null);
    }

    public static int asIntExact(AnyValue value, Supplier<String> contextForErrorMessage) {
        final long longValue = asLong(value, contextForErrorMessage);
        final int intValue = (int) longValue;
        if (intValue != longValue) {
            String errorMsg = format(
                    "Expected an integer between %d and %d, but got: %d",
                    Integer.MIN_VALUE, Integer.MAX_VALUE, longValue);
            if (contextForErrorMessage != null) {
                errorMsg = contextForErrorMessage.get() + ": " + errorMsg;
            }
            throw new IllegalArgumentException(errorMsg);
        }
        return intValue;
    }

    public static long nodeId(AnyValue value) {
        assert value != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if (value instanceof VirtualNodeValue) {
            return ((VirtualNodeValue) value).id();
        } else {
            if (value instanceof Value v)
                throw CypherTypeException.expectedVirtualNode(
                        v.prettyPrint(), value.getClass().getName(), CypherTypeValueMapper.valueType(value));
            else
                throw CypherTypeException.expectedVirtualNode(
                        String.valueOf(value), value.getClass().getName(), CypherTypeValueMapper.valueType(value));
        }
    }

    public static AnyValue isNormalized(AnyValue input, NormalForm normalForm) {
        if (input == NO_VALUE) {
            return NO_VALUE;
        }

        if (input instanceof TextValue asText) {
            Normalizer.Form form;
            try {
                form = Normalizer.Form.valueOf(normalForm.description());
            } catch (IllegalArgumentException e) {
                throw InvalidArgumentException.unknownNormalForm(String.valueOf(normalForm));
            }
            boolean normalized = Normalizer.isNormalized(asText.stringValue(), form);
            return Values.booleanValue(normalized);
        } else {
            return NO_VALUE;
        }
    }

    public static BooleanValue isTyped(AnyValue item, CypherType typeName) {
        boolean result;
        if (typeName instanceof NothingType) {
            result = false;
        } else if (item instanceof NoValue) {
            result = typeName instanceof NullType || typeName.isNullable();
        } else if (typeName instanceof NullType) {
            result = false;
        } else if (typeName instanceof AnyType) {
            result = true;
        } else if (typeName instanceof ListType listType) {
            result = (item instanceof SequenceValue list) && checkInnerListIsTyped(list, listType);
        } else if (typeName.hasValueRepresentation()) {
            result = possibleValueRepresentations(typeName).contains(item.valueRepresentation());
        } else if (typeName instanceof NodeType) {
            result = item instanceof VirtualNodeValue;
        } else if (typeName instanceof RelationshipType) {
            result = item instanceof VirtualRelationshipValue;
        } else if (typeName instanceof MapType) {
            result = item instanceof MapValue;
        } else if (typeName instanceof PathType) {
            result = item instanceof VirtualPathValue;
        } else if (typeName instanceof PropertyValueType) {
            result = hasPropertyValueRepresentation(item.valueRepresentation())
                    || (item instanceof ListValue listValue
                            && (listValue.isEmpty()
                                    || hasPropertyValueRepresentation(listValue.itemValueRepresentation())));
        } else if (typeName instanceof ClosedDynamicUnionType unionType) {
            result = false;
            for (CypherType innerType : asJava(unionType.innerTypes())) {
                if (isTyped(item, innerType) == TRUE) {
                    result = true;
                    break;
                }
            }
        } else {
            throw new IllegalArgumentException(String.format("Unexpected type: %s", typeName.toCypherTypeString()));
        }

        return Values.booleanValue(result);
    }

    public static final CypherTypeValueMapper CYPHER_TYPE_NAME_VALUE_MAPPER = new CypherTypeValueMapper();

    public static Value valueType(AnyValue in) {
        return Values.stringValue(
                CypherType.normalizeTypes(in.map(CYPHER_TYPE_NAME_VALUE_MAPPER)).description());
    }

    private static boolean hasPropertyValueRepresentation(ValueRepresentation valueRepresentation) {
        return !valueRepresentation.equals(ValueRepresentation.ANYTHING)
                && !valueRepresentation.equals(ValueRepresentation.UNKNOWN)
                && !valueRepresentation.equals(ValueRepresentation.NO_VALUE);
    }

    private static List<ValueRepresentation> possibleValueRepresentations(CypherType cypherType)
            throws UnsupportedOperationException {
        if (cypherType instanceof BooleanType) {
            return List.of(ValueRepresentation.BOOLEAN);
        } else if (cypherType instanceof StringType) {
            return List.of(ValueRepresentation.UTF8_TEXT, ValueRepresentation.UTF16_TEXT);
        } else if (cypherType instanceof IntegerType) {
            return List.of(
                    ValueRepresentation.INT8,
                    ValueRepresentation.INT16,
                    ValueRepresentation.INT32,
                    ValueRepresentation.INT64);
        } else if (cypherType instanceof FloatType) {
            return List.of(ValueRepresentation.FLOAT32, ValueRepresentation.FLOAT64);
        } else if (cypherType instanceof NumberType) {
            return List.of(
                    ValueRepresentation.INT8,
                    ValueRepresentation.INT16,
                    ValueRepresentation.INT32,
                    ValueRepresentation.INT64,
                    ValueRepresentation.FLOAT32,
                    ValueRepresentation.FLOAT64);
        } else if (cypherType instanceof DateType) {
            return List.of(ValueRepresentation.DATE);
        } else if (cypherType instanceof LocalTimeType) {
            return List.of(ValueRepresentation.LOCAL_TIME);
        } else if (cypherType instanceof ZonedTimeType) {
            return List.of(ValueRepresentation.ZONED_TIME);
        } else if (cypherType instanceof LocalDateTimeType) {
            return List.of(ValueRepresentation.LOCAL_DATE_TIME);
        } else if (cypherType instanceof ZonedDateTimeType) {
            return List.of(ValueRepresentation.ZONED_DATE_TIME);
        } else if (cypherType instanceof DurationType) {
            return List.of(ValueRepresentation.DURATION);
        } else if (cypherType instanceof GeometryType || cypherType instanceof PointType) {
            return List.of(ValueRepresentation.GEOMETRY);
        } else if (cypherType instanceof ListType listType) {
            if (listType.innerType() instanceof BooleanType) {
                return List.of(ValueRepresentation.BOOLEAN_ARRAY);
            } else if (listType.innerType() instanceof StringType) {
                return List.of(ValueRepresentation.TEXT_ARRAY);
            } else if (listType.innerType() instanceof IntegerType) {
                return List.of(
                        ValueRepresentation.INT8_ARRAY,
                        ValueRepresentation.INT16_ARRAY,
                        ValueRepresentation.INT32_ARRAY,
                        ValueRepresentation.INT64_ARRAY);
            } else if (listType.innerType() instanceof FloatType) {
                return List.of(ValueRepresentation.FLOAT32_ARRAY, ValueRepresentation.FLOAT64_ARRAY);
            } else if (listType.innerType() instanceof NumberType) {
                return List.of(
                        ValueRepresentation.INT8_ARRAY,
                        ValueRepresentation.INT16_ARRAY,
                        ValueRepresentation.INT32_ARRAY,
                        ValueRepresentation.INT64_ARRAY,
                        ValueRepresentation.FLOAT32_ARRAY,
                        ValueRepresentation.FLOAT64_ARRAY);
            } else if (listType.innerType() instanceof DateType) {
                return List.of(ValueRepresentation.DATE_ARRAY);
            } else if (listType.innerType() instanceof LocalTimeType) {
                return List.of(ValueRepresentation.LOCAL_TIME_ARRAY);
            } else if (listType.innerType() instanceof ZonedTimeType) {
                return List.of(ValueRepresentation.ZONED_TIME_ARRAY);
            } else if (listType.innerType() instanceof LocalDateTimeType) {
                return List.of(ValueRepresentation.LOCAL_DATE_TIME_ARRAY);
            } else if (listType.innerType() instanceof ZonedDateTimeType) {
                return List.of(ValueRepresentation.ZONED_DATE_TIME_ARRAY);
            } else if (listType.innerType() instanceof DurationType) {
                return List.of(ValueRepresentation.DURATION_ARRAY);
            } else if (listType.innerType() instanceof PointType || listType.innerType() instanceof GeometryType) {
                return List.of(ValueRepresentation.GEOMETRY_ARRAY);
            } else {
                return List.of();
            }
        } else {
            throw new UnsupportedOperationException(String.format(
                    "possibleValueRepresentations not supported on %s",
                    cypherType.getClass().getName()));
        }
    }

    private static boolean checkInnerListIsTyped(SequenceValue values, ListType typeName) {
        final var itemType = typeName.innerType();
        // An empty list can be a list of anything, even NOTHING, so don't check further
        // A list of LIST<ANY> can also be anything, so no need to check further
        if (values.isEmpty() || (!itemType.isNullable() && itemType instanceof AnyType)) return true;
        // A non-empty list of NOTHING is always false
        if (itemType instanceof NothingType) return false;
        if (values instanceof ArrayValue array) {
            // An ArrayValue can only hold storable types (not null)
            // So a LIST<ANY [NOT NULL]>, LIST<PROPERTY VALUE [NOT NULL]> are true
            // else check that the specific array type matches
            return itemType instanceof AnyType
                    || itemType instanceof PropertyValueType
                    || (typeName.hasValueRepresentation()
                            && possibleValueRepresentations(typeName).contains(array.valueRepresentation()));
        } else if (values instanceof ListValue list) {
            // For a simple LIST<TYPE NOT NULL> we can quickly check the list type
            // without needing to iterate over the list
            // Lists that are mixed ints and floats will return as a list of float here, so don't allow the shortcut for
            // that
            if (itemType.hasValueRepresentation()
                    && !itemType.isNullable()
                    && list.itemValueRepresentation().valueGroup() != ValueGroup.NUMBER
                    && possibleValueRepresentations(itemType).contains(list.itemValueRepresentation())) {
                return true;
            } else {
                // The list is either mixed, or may contain nulls, must check all values
                for (AnyValue value : values) {
                    if (isTyped(value, itemType) == FALSE) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    public static AnyValue assertIsNode(AnyValue item) {
        if (item == NO_VALUE) {
            return NO_VALUE;
        } else if (item instanceof VirtualNodeValue) {
            return TRUE;
        } else {
            if (item instanceof Value v)
                throw CypherTypeException.expectedNode(
                        String.valueOf(v), v.prettyPrint(), CypherTypeValueMapper.valueType(v));
            else
                throw CypherTypeException.expectedNode(
                        String.valueOf(item), String.valueOf(item), CypherTypeValueMapper.valueType(item));
        }
    }

    private static CypherTypeException needsNumbers(String method) {
        return new CypherTypeException(format("%s requires numbers", method));
    }

    private static CypherTypeException notAString(String method, AnyValue in) {
        return new CypherTypeException(format(
                "Expected a string value for `%s`, but got: %s; consider converting it to a string with "
                        + "toString().",
                method, in));
    }

    private static CypherTypeException notAModeString(String method, AnyValue mode) {
        return new CypherTypeException(format("Expected a string value for `%s`, but got: %s.", method, mode));
    }

    private static ListValue toIntegerList(FloatingPointArray array) {
        var converted = ListValueBuilder.newListBuilder(array.intSize());
        for (int i = 0; i < array.intSize(); i++) {
            converted.add(longValue((long) array.doubleValue(i)));
        }
        return converted.build();
    }

    private static ListValue toIntegerList(SequenceValue sequenceValue) {
        var converted = ListValueBuilder.newListBuilder();
        for (AnyValue value : sequenceValue) {
            converted.add(value != NO_VALUE ? toIntegerOrNull(value) : NO_VALUE);
        }
        return converted.build();
    }

    private static Consumer<RelationshipVisitor> consumer(DbAccess access, RelationshipScanCursor cursor) {
        return relationshipVisitor -> {
            access.singleRelationship(relationshipVisitor.id(), cursor);
            if (cursor.next()) {
                relationshipVisitor.visit(cursor.sourceNodeReference(), cursor.targetNodeReference(), cursor.type());
            }
        };
    }
}
