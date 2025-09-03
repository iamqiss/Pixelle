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
import org.neo4j.internal.schema.IndexConfigValidationRecords.UnrecognizedSetting;
import org.neo4j.internal.schema.SettingsAccessor;
import org.neo4j.internal.schema.SettingsAccessor.IndexConfigAccessor;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexConfig.HnswConfig;
import org.neo4j.kernel.api.schema.vector.VectorTestUtils.VectorIndexSettings;
import org.neo4j.kernel.api.vector.VectorSimilarityFunction;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;

class VectorIndexV2ForV523ConfigValidationTest {
    private static final VectorIndexVersion VERSION = VectorIndexVersion.V2_0;
    private static final VectorIndexSettingsValidator VALIDATOR = VERSION.indexSettingValidator(KernelVersion.V5_23);

    @Test
    void validV2ForV518IndexConfig() {
        final var settings = VectorIndexSettings.create()
                .withDimensions(VERSION.maxDimensions())
                .withSimilarityFunction(VERSION.similarityFunction("COSINE"))
                .toSettingsAccessor();

        final var vectorIndexConfigAsIfCreatedOn518 =
                VERSION.indexSettingValidator(KernelVersion.V5_18).validateToVectorIndexConfig(settings);

        final var vectorIndexConfig = VALIDATOR.trustIsValidToVectorIndexConfig(
                new IndexConfigAccessor(vectorIndexConfigAsIfCreatedOn518.config()));

        assertThat(vectorIndexConfig).isEqualTo(vectorIndexConfigAsIfCreatedOn518);
    }

    @Test
    void validIndexConfig() {
        final var settings = VectorIndexSettings.create()
                .withDimensions(VERSION.maxDimensions())
                .withHnswM(16)
                .withHnswEfConstruction(100)
                .withQuantizationDisabled()
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
                .containsExactlyInAnyOrder(
                        DIMENSIONS.getSettingName(),
                        SIMILARITY_FUNCTION.getSettingName(),
                        QUANTIZATION_ENABLED.getSettingName(),
                        HNSW_M.getSettingName(),
                        HNSW_EF_CONSTRUCTION.getSettingName());
    }

    @Test
    void validIndexConfigWithDefaults() {
        final var settings = VectorIndexSettings.create().toSettingsAccessor();

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
                        OptionalInt.empty(), VERSION.similarityFunction("COSINE"), true, new HnswConfig(16, 100));

        assertThat(vectorIndexConfig.config().entries().collect(Pair::getOne))
                .containsExactlyInAnyOrder(
                        SIMILARITY_FUNCTION.getSettingName(),
                        QUANTIZATION_ENABLED.getSettingName(),
                        HNSW_M.getSettingName(),
                        HNSW_EF_CONSTRUCTION.getSettingName());
    }

    @Test
    void unrecognisedSetting() {
        final var unrecognisedSetting = IndexSetting.fulltext_Analyzer();
        final var settings =
                VectorIndexSettings.create().set(unrecognisedSetting, "swedish").toSettingsAccessor();

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
    void nullDimensions() {
        final var settings = VectorIndexSettings.create().set(DIMENSIONS, null).toSettingsAccessor();

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
        final var settings =
                VectorIndexSettings.create().withDimensions(invalidDimensions).toSettingsAccessor();

        assertInvalidDimensions(invalidDimensions, settings);
    }

    @Test
    void aboveMaxDimensions() {
        final int invalidDimensions = VERSION.maxDimensions() + 1;
        final var settings =
                VectorIndexSettings.create().withDimensions(invalidDimensions).toSettingsAccessor();

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
    void nullSimilarityFunction() {
        final var settings =
                VectorIndexSettings.create().set(SIMILARITY_FUNCTION, null).toSettingsAccessor();

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
    void incorrectTypeForQuantizationEnabled() {
        final var incorrectQuantizationEnabled = 123L;
        final var settings = VectorIndexSettings.create()
                .set(QUANTIZATION_ENABLED, incorrectQuantizationEnabled)
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
                .containsExactly(QUANTIZATION_ENABLED, Values.longValue(incorrectQuantizationEnabled));
        incorrectTypeAssert
                .extracting(IncorrectType::providedType)
                .asInstanceOf(InstanceOfAssertFactories.CLASS)
                .isAssignableTo(NumberValue.class);
        incorrectTypeAssert
                .extracting(IncorrectType::targetType)
                .asInstanceOf(InstanceOfAssertFactories.CLASS)
                .isAssignableTo(BooleanValue.class);

        assertThatThrownBy(() -> VALIDATOR.validateToVectorIndexConfig(settings))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll(
                        QUANTIZATION_ENABLED.getSettingName(),
                        "is expected to have been",
                        BooleanValue.class.getSimpleName());
    }

    @Test
    void nullHnswM() {
        final var settings = VectorIndexSettings.create().set(HNSW_M, null).toSettingsAccessor();

        final var validationRecords = VALIDATOR.validate(settings);
        assertThat(validationRecords.invalid()).isTrue();
        assertThat(validationRecords.get(INVALID_VALUE).castToSortedSet())
                .hasSize(1)
                .first()
                .asInstanceOf(InstanceOfAssertFactories.type(InvalidValue.class))
                .extracting(InvalidValue::setting, InvalidValue::value)
                .containsExactly(HNSW_M, null);

        assertThatThrownBy(() -> VALIDATOR.validateToVectorIndexConfig(settings))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll(
                        HNSW_M.getSettingName(), "must be between 1 and", String.valueOf(VERSION.maxHnswM()));
    }

    @Test
    void incorrectTypeForHnswM() {
        final var incorrectHnswM = "Here is a String";
        final var settings =
                VectorIndexSettings.create().set(HNSW_M, incorrectHnswM).toSettingsAccessor();

        final var validationRecords = VALIDATOR.validate(settings);
        assertThat(validationRecords.invalid()).isTrue();
        final var incorrectTypeAssert = assertThat(
                        validationRecords.get(INCORRECT_TYPE).castToSortedSet())
                .hasSize(1)
                .first()
                .asInstanceOf(InstanceOfAssertFactories.type(IncorrectType.class));
        incorrectTypeAssert
                .extracting(IncorrectType::setting, IncorrectType::rawValue)
                .containsExactly(HNSW_M, Values.stringValue(incorrectHnswM));
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
                        HNSW_M.getSettingName(), "is expected to have been", IntegralValue.class.getSimpleName());
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0})
    void nonPositiveHnswM(int invalidM) {
        final var settings = VectorIndexSettings.create()
                .withDimensions(VERSION.maxDimensions())
                .withSimilarityFunction(VERSION.similarityFunction("COSINE"))
                .withHnswM(invalidM)
                .toSettingsAccessor();

        assertInvalidM(invalidM, settings);
    }

    @Test
    void aboveMaxHnswM() {
        final int invalidHnswM = VERSION.maxHnswM() + 1;
        final var settings = VectorIndexSettings.create()
                .withDimensions(VERSION.maxDimensions())
                .withSimilarityFunction(VERSION.similarityFunction("COSINE"))
                .withHnswM(invalidHnswM)
                .toSettingsAccessor();

        assertInvalidM(invalidHnswM, settings);
    }

    private void assertInvalidM(int invalidM, SettingsAccessor settings) {
        final var validationRecords = VALIDATOR.validate(settings);
        assertThat(validationRecords.invalid()).isTrue();
        assertThat(validationRecords.get(INVALID_VALUE).castToSortedSet())
                .hasSize(1)
                .first()
                .asInstanceOf(InstanceOfAssertFactories.type(InvalidValue.class))
                .extracting(InvalidValue::setting, InvalidValue::value)
                .containsExactly(HNSW_M, invalidM);

        assertThatThrownBy(() -> VALIDATOR.validateToVectorIndexConfig(settings))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll(
                        HNSW_M.getSettingName(), "must be between 1 and", String.valueOf(VERSION.maxHnswM()));
    }

    @Test
    void nullHnswEfConstruction() {
        final var settings =
                VectorIndexSettings.create().set(HNSW_EF_CONSTRUCTION, null).toSettingsAccessor();

        final var validationRecords = VALIDATOR.validate(settings);
        assertThat(validationRecords.invalid()).isTrue();
        assertThat(validationRecords.get(INVALID_VALUE).castToSortedSet())
                .hasSize(1)
                .first()
                .asInstanceOf(InstanceOfAssertFactories.type(InvalidValue.class))
                .extracting(InvalidValue::setting, InvalidValue::value)
                .containsExactly(HNSW_EF_CONSTRUCTION, null);

        assertThatThrownBy(() -> VALIDATOR.validateToVectorIndexConfig(settings))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll(
                        HNSW_EF_CONSTRUCTION.getSettingName(),
                        "must be between 1 and",
                        String.valueOf(VERSION.maxHnswEfConstruction()));
    }

    @Test
    void incorrectTypeForHnswEfConstruction() {
        final var incorrectHnswEfConstruction = "Here is a String";
        final var settings = VectorIndexSettings.create()
                .set(HNSW_EF_CONSTRUCTION, incorrectHnswEfConstruction)
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
                .containsExactly(HNSW_EF_CONSTRUCTION, Values.stringValue(incorrectHnswEfConstruction));
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
                        HNSW_EF_CONSTRUCTION.getSettingName(),
                        "is expected to have been",
                        IntegralValue.class.getSimpleName());
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0})
    void nonPositiveHnswEfConstruction(int invalidEfConstruction) {
        final var settings = VectorIndexSettings.create()
                .withHnswEfConstruction(invalidEfConstruction)
                .toSettingsAccessor();

        assertInvalidEfConstruction(invalidEfConstruction, settings);
    }

    @Test
    void aboveMaxHnswEfConstruction() {
        final int invalidHnswEfConstruction = VERSION.maxHnswEfConstruction() + 1;
        final var settings = VectorIndexSettings.create()
                .withHnswEfConstruction(invalidHnswEfConstruction)
                .toSettingsAccessor();

        assertInvalidEfConstruction(invalidHnswEfConstruction, settings);
    }

    private void assertInvalidEfConstruction(int invalidHnswEfConstruction, SettingsAccessor settings) {
        final var validationRecords = VALIDATOR.validate(settings);
        assertThat(validationRecords.invalid()).isTrue();
        assertThat(validationRecords.get(INVALID_VALUE).castToSortedSet())
                .hasSize(1)
                .first()
                .asInstanceOf(InstanceOfAssertFactories.type(InvalidValue.class))
                .extracting(InvalidValue::setting, InvalidValue::value)
                .containsExactly(HNSW_EF_CONSTRUCTION, invalidHnswEfConstruction);

        assertThatThrownBy(() -> VALIDATOR.validateToVectorIndexConfig(settings))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll(
                        HNSW_EF_CONSTRUCTION.getSettingName(),
                        "must be between 1 and",
                        String.valueOf(VERSION.maxHnswEfConstruction()));
    }
}
