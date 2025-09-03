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
package org.neo4j.kernel.api.impl.schema.vector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.internal.schema.IndexConfigValidationRecords.State.INCORRECT_TYPE;
import static org.neo4j.internal.schema.IndexConfigValidationRecords.State.INVALID_VALUE;
import static org.neo4j.internal.schema.IndexConfigValidationRecords.State.MISSING_SETTING;
import static org.neo4j.internal.schema.IndexConfigValidationRecords.State.UNRECOGNIZED_SETTING;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.DIMENSIONS;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.HNSW_EF_CONSTRUCTION;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.HNSW_M;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.QUANTIZATION_ENABLED;
import static org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfigUtils.SIMILARITY_FUNCTION;

import java.util.OptionalInt;
import org.apache.commons.lang3.mutable.MutableObject;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.eclipse.collections.api.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.schema.IndexConfigValidationRecords.IncorrectType;
import org.neo4j.internal.schema.IndexConfigValidationRecords.InvalidValue;
import org.neo4j.internal.schema.IndexConfigValidationRecords.MissingSetting;
import org.neo4j.internal.schema.IndexConfigValidationRecords.UnrecognizedSetting;
import org.neo4j.internal.schema.SettingsAccessor;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfig.HnswConfig;
import org.neo4j.kernel.api.schema.vector.VectorTestUtils.VectorIndexSettings;
import org.neo4j.kernel.api.vector.VectorSimilarityFunction;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;

class VectorIndexV2ForV518ConfigValidationTest {
    private static final VectorIndexVersion VERSION = VectorIndexVersion.V2_0;
    private static final VectorIndexSettingsValidator VALIDATOR = VERSION.indexSettingValidator(KernelVersion.V5_18);

    @Test
    void validIndexConfig() {
        final var settings = VectorIndexSettings.create()
                .withDimensions(VERSION.maxDimensions())
                .withSimilarityFunction(VERSION.similarityFunction("COSINE"))
                .toSettingsAccessor();

        final var validationRecords = VALIDATOR.validate(settings);
        assertThat(validationRecords.valid()).isTrue();

        final var ref = new MutableObject<VectorIndexConfig>();
        assertThatCode(() -> ref.setValue(VALIDATOR.validateToVectorIndexConfig(settings)))
                .doesNotThrowAnyException();
        final var vectorIndexConfig = ref.getValue();

        assertThat(vectorIndexConfig)
                .extracting(
                        VectorIndexConfig::dimensions,
                        VectorIndexConfig::similarityFunction,
                        VectorIndexConfig::quantizationEnabled,
                        VectorIndexConfig::hnsw)
                .containsExactly(
                        OptionalInt.of(VERSION.maxDimensions()),
                        VERSION.similarityFunction("COSINE"),
                        false,
                        new HnswConfig(16, 100));

        assertThat(vectorIndexConfig.config().entries().collect(Pair::getOne))
                .containsExactlyInAnyOrder(DIMENSIONS.getSettingName(), SIMILARITY_FUNCTION.getSettingName());
    }

    @Test
    void unrecognisedSetting() {
        final var unrecognisedSetting = IndexSetting.fulltext_Analyzer();
        final var settings = VectorIndexSettings.create()
                .withDimensions(VERSION.maxDimensions())
                .withSimilarityFunction(VERSION.similarityFunction("COSINE"))
                .set(unrecognisedSetting, "swedish")
                .toSettingsAccessor();

        final var validationRecords = VALIDATOR.validate(settings);
        assertThat(validationRecords.invalid()).isTrue();
        assertThat(validationRecords.get(UNRECOGNIZED_SETTING).castToSortedSet())
                .hasSize(1)
                .first()
                .asInstanceOf(InstanceOfAssertFactories.type(UnrecognizedSetting.class))
                .extracting(UnrecognizedSetting::settingName)
                .isEqualTo(unrecognisedSetting.getSettingName());

        assertThatThrownBy(() -> VALIDATOR.validateToVectorIndexConfig(settings))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll(
                        unrecognisedSetting.getSettingName(),
                        "is an unrecognized setting for index with provider",
                        VERSION.descriptor().name());
    }

    @Test
    void missingDimensions() {
        final var settings = VectorIndexSettings.create()
                .withSimilarityFunction(VERSION.similarityFunction("COSINE"))
                .toSettingsAccessor();

        final var validationRecords = VALIDATOR.validate(settings);
        assertThat(validationRecords.invalid()).isTrue();
        assertThat(validationRecords.get(MISSING_SETTING).castToSortedSet())
                .hasSize(1)
                .first()
                .asInstanceOf(InstanceOfAssertFactories.type(MissingSetting.class))
                .extracting(MissingSetting::setting)
                .isEqualTo(DIMENSIONS);

        assertThatThrownBy(() -> VALIDATOR.validateToVectorIndexConfig(settings))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll(DIMENSIONS.getSettingName(), "is expected to have been set");
    }

    @Test
    void nullDimensions() {
        final var settings = VectorIndexSettings.create()
                .set(DIMENSIONS, null)
                .withSimilarityFunction(VERSION.similarityFunction("COSINE"))
                .toSettingsAccessor();

        final var validationRecords = VALIDATOR.validate(settings);
        assertThat(validationRecords.invalid()).isTrue();
        assertThat(validationRecords.get(INVALID_VALUE).castToSortedSet())
                .hasSize(1)
                .first()
                .asInstanceOf(InstanceOfAssertFactories.type(InvalidValue.class))
                .extracting(InvalidValue::setting, InvalidValue::value)
                .containsExactly(DIMENSIONS, null);

        assertThatThrownBy(() -> VALIDATOR.validateToVectorIndexConfig(settings))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll(
                        DIMENSIONS.getSettingName(), "must be between 1 and", String.valueOf(VERSION.maxDimensions()));
    }

    @Test
    void incorrectTypeForDimensions() {
        final var incorrectDimensions = String.valueOf(VERSION.maxDimensions());
        final var settings = VectorIndexSettings.create()
                .set(DIMENSIONS, incorrectDimensions)
                .withSimilarityFunction(VERSION.similarityFunction("COSINE"))
                .toSettingsAccessor();

        final var validationRecords = VALIDATOR.validate(settings);
        assertThat(validationRecords.invalid()).isTrue();
        final var incorrectTypeAssert = assertThat(
                        validationRecords.get(INCORRECT_TYPE).castToSortedSet())
                .hasSize(1)
                .first()
                .asInstanceOf(InstanceOfAssertFactories.type(IncorrectType.class));
        incorrectTypeAssert
                .extracting(IncorrectType::setting, IncorrectType::rawValue)
                .containsExactly(DIMENSIONS, Values.stringValue(incorrectDimensions));
        incorrectTypeAssert
                .extracting(IncorrectType::providedType)
                .asInstanceOf(InstanceOfAssertFactories.CLASS)
                .isAssignableTo(TextValue.class);
        incorrectTypeAssert
                .extracting(IncorrectType::targetType)
                .asInstanceOf(InstanceOfAssertFactories.CLASS)
                .isAssignableTo(IntegralValue.class);

        assertThatThrownBy(() -> VALIDATOR.validateToVectorIndexConfig(settings))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll(
                        DIMENSIONS.getSettingName(), "is expected to have been", IntegralValue.class.getSimpleName());
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0})
    void nonPositiveDimensions(int invalidDimensions) {
        final var settings = VectorIndexSettings.create()
                .withDimensions(invalidDimensions)
                .withSimilarityFunction(VERSION.similarityFunction("COSINE"))
                .toSettingsAccessor();

        assertInvalidDimensions(invalidDimensions, settings);
    }

    @Test
    void aboveMaxDimensions() {
        final int invalidDimensions = VERSION.maxDimensions() + 1;
        final var settings = VectorIndexSettings.create()
                .withDimensions(invalidDimensions)
                .withSimilarityFunction(VERSION.similarityFunction("COSINE"))
                .toSettingsAccessor();

        assertInvalidDimensions(invalidDimensions, settings);
    }

    private void assertInvalidDimensions(int invalidDimensions, SettingsAccessor settings) {
        final var validationRecords = VALIDATOR.validate(settings);
        assertThat(validationRecords.invalid()).isTrue();
        assertThat(validationRecords.get(INVALID_VALUE).castToSortedSet())
                .hasSize(1)
                .first()
                .asInstanceOf(InstanceOfAssertFactories.type(InvalidValue.class))
                .extracting(InvalidValue::setting, InvalidValue::value)
                .containsExactly(DIMENSIONS, OptionalInt.of(invalidDimensions));

        assertThatThrownBy(() -> VALIDATOR.validateToVectorIndexConfig(settings))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll(
                        DIMENSIONS.getSettingName(), "must be between 1 and", String.valueOf(VERSION.maxDimensions()));
    }

    @Test
    void missingSimilarityFunction() {
        final var settings = VectorIndexSettings.create()
                .withDimensions(VERSION.maxDimensions())
                .toSettingsAccessor();

        final var validationRecords = VALIDATOR.validate(settings);
        assertThat(validationRecords.invalid()).isTrue();
        assertThat(validationRecords.get(MISSING_SETTING).castToSortedSet())
                .hasSize(1)
                .first()
                .asInstanceOf(InstanceOfAssertFactories.type(MissingSetting.class))
                .extracting(MissingSetting::setting)
                .isEqualTo(SIMILARITY_FUNCTION);

        assertThatThrownBy(() -> VALIDATOR.validateToVectorIndexConfig(settings))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll(SIMILARITY_FUNCTION.getSettingName(), "is expected to have been set");
    }

    @Test
    void nullSimilarityFunction() {
        final var settings = VectorIndexSettings.create()
                .withDimensions(VERSION.maxDimensions())
                .set(SIMILARITY_FUNCTION, null)
                .toSettingsAccessor();

        final var validationRecords = VALIDATOR.validate(settings);
        assertThat(validationRecords.invalid()).isTrue();
        assertThat(validationRecords.get(INVALID_VALUE).castToSortedSet())
                .hasSize(1)
                .first()
                .asInstanceOf(InstanceOfAssertFactories.type(InvalidValue.class))
                .extracting(InvalidValue::setting, InvalidValue::value)
                .containsExactly(SIMILARITY_FUNCTION, null);

        assertThatThrownBy(() -> VALIDATOR.validateToVectorIndexConfig(settings))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll(
                        "null",
                        "is an unsupported",
                        SIMILARITY_FUNCTION.getSettingName(),
                        VERSION.supportedSimilarityFunctions()
                                .collect(VectorSimilarityFunction::name)
                                .toString());
    }

    @Test
    void incorrectTypeForSimilarityFunction() {
        final var incorrectSimilarityFunction = 123L;
        final var settings = VectorIndexSettings.create()
                .withDimensions(VERSION.maxDimensions())
                .set(SIMILARITY_FUNCTION, incorrectSimilarityFunction)
                .toSettingsAccessor();

        final var validationRecords = VALIDATOR.validate(settings);
        assertThat(validationRecords.invalid()).isTrue();
        final var incorrectTypeAssert = assertThat(
                        validationRecords.get(INCORRECT_TYPE).castToSortedSet())
                .hasSize(1)
                .first()
                .asInstanceOf(InstanceOfAssertFactories.type(IncorrectType.class));
        incorrectTypeAssert
                .extracting(IncorrectType::setting, IncorrectType::rawValue)
                .containsExactly(SIMILARITY_FUNCTION, Values.longValue(incorrectSimilarityFunction));
        incorrectTypeAssert
                .extracting(IncorrectType::providedType)
                .asInstanceOf(InstanceOfAssertFactories.CLASS)
                .isAssignableTo(NumberValue.class);
        incorrectTypeAssert
                .extracting(IncorrectType::targetType)
                .asInstanceOf(InstanceOfAssertFactories.CLASS)
                .isAssignableTo(TextValue.class);

        assertThatThrownBy(() -> VALIDATOR.validateToVectorIndexConfig(settings))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll(
                        SIMILARITY_FUNCTION.getSettingName(),
                        "is expected to have been",
                        TextValue.class.getSimpleName());
    }

    @Test
    void invalidSimilarityFunction() {
        final var invalidSimilarityFunction = "ClearlyThisIsNotASimilarityFunction";
        final var settings = VectorIndexSettings.create()
                .withDimensions(VERSION.maxDimensions())
                .set(IndexSetting.vector_Similarity_Function(), invalidSimilarityFunction)
                .toSettingsAccessor();

        final var validationRecords = VALIDATOR.validate(settings);
        assertThat(validationRecords.invalid()).isTrue();
        assertThat(validationRecords.get(INVALID_VALUE).castToSortedSet())
                .hasSize(1)
                .first()
                .asInstanceOf(InstanceOfAssertFactories.type(InvalidValue.class))
                .extracting(InvalidValue::setting, InvalidValue::rawValue)
                .containsExactly(SIMILARITY_FUNCTION, Values.stringValue(invalidSimilarityFunction));

        assertThatThrownBy(() -> VALIDATOR.validateToVectorIndexConfig(settings))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll(
                        invalidSimilarityFunction,
                        "is an unsupported",
                        SIMILARITY_FUNCTION.getSettingName(),
                        VERSION.supportedSimilarityFunctions()
                                .collect(VectorSimilarityFunction::name)
                                .toString());
    }

    @Test
    void cannotSetQuantizationEnabled() {
        final var settings = VectorIndexSettings.create()
                .withDimensions(VERSION.maxDimensions())
                .withSimilarityFunction(VERSION.similarityFunction("COSINE"))
                .withQuantizationDisabled()
                .toSettingsAccessor();

        final var validationRecords = VALIDATOR.validate(settings);
        assertThat(validationRecords.invalid()).isTrue();
        assertThat(validationRecords.get(UNRECOGNIZED_SETTING).castToSortedSet())
                .hasSize(1)
                .first()
                .asInstanceOf(InstanceOfAssertFactories.type(UnrecognizedSetting.class))
                .extracting(UnrecognizedSetting::settingName)
                .isEqualTo(QUANTIZATION_ENABLED.getSettingName());
    }

    @Test
    void cannotSetHnswM() {
        final var settings = VectorIndexSettings.create()
                .withDimensions(VERSION.maxDimensions())
                .withSimilarityFunction(VERSION.similarityFunction("COSINE"))
                .withHnswM(16)
                .toSettingsAccessor();

        final var validationRecords = VALIDATOR.validate(settings);
        assertThat(validationRecords.invalid()).isTrue();
        assertThat(validationRecords.get(UNRECOGNIZED_SETTING).castToSortedSet())
                .hasSize(1)
                .first()
                .asInstanceOf(InstanceOfAssertFactories.type(UnrecognizedSetting.class))
                .extracting(UnrecognizedSetting::settingName)
                .isEqualTo(HNSW_M.getSettingName());
    }

    @Test
    void cannotSetHnswEfConstruction() {
        final var settings = VectorIndexSettings.create()
                .withDimensions(VERSION.maxDimensions())
                .withSimilarityFunction(VERSION.similarityFunction("COSINE"))
                .withHnswEfConstruction(100)
                .toSettingsAccessor();

        final var validationRecords = VALIDATOR.validate(settings);
        assertThat(validationRecords.invalid()).isTrue();
        assertThat(validationRecords.get(UNRECOGNIZED_SETTING).castToSortedSet())
                .hasSize(1)
                .first()
                .asInstanceOf(InstanceOfAssertFactories.type(UnrecognizedSetting.class))
                .extracting(UnrecognizedSetting::settingName)
                .isEqualTo(HNSW_EF_CONSTRUCTION.getSettingName());
    }
}
