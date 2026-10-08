/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.quarkus.debezium.deployment.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

class GeneratedIdentifierTest {

    private static final UUID ID = UUID.fromString("abcdefab-1234-5678-9abc-def012345678");
    private static final UUID ID_WITHOUT_DIGITS_IN_FIRST_SEGMENT = UUID.fromString("abcdefab-0000-0000-0000-000000000000");
    private static final UUID ID_WITH_SAME_LEADING_DIGITS = UUID.fromString("12abcdef-1111-1111-1111-111111111111");
    private static final UUID ANOTHER_ID_WITH_SAME_LEADING_DIGITS = UUID.fromString("12cdefab-2222-2222-2222-222222222222");

    @Test
    @FixFor("debezium/dbz#2726")
    @DisplayName("should keep all the characters of the uuid without dashes")
    void shouldKeepAllUuidCharacters() {
        assertThat(GeneratedIdentifier.of(ID)).isEqualTo("abcdefab123456789abcdef012345678");
    }

    @Test
    @FixFor("debezium/dbz#2726")
    @DisplayName("should not be empty when the first segment of the uuid has no digits")
    void shouldNotBeEmptyWhenFirstSegmentHasNoDigits() {
        assertThat(GeneratedIdentifier.of(ID_WITHOUT_DIGITS_IN_FIRST_SEGMENT)).isNotEmpty();
    }

    @Test
    @FixFor("debezium/dbz#2726")
    @DisplayName("should be different when uuids share the same leading digits")
    void shouldBeDifferentWhenUuidsShareTheSameLeadingDigits() {
        assertThat(GeneratedIdentifier.of(ID_WITH_SAME_LEADING_DIGITS))
                .isNotEqualTo(GeneratedIdentifier.of(ANOTHER_ID_WITH_SAME_LEADING_DIGITS));
    }

    @Test
    @FixFor("debezium/dbz#2726")
    @DisplayName("should be used as short identifier by the generated class metadata")
    void shouldBeUsedByGeneratedClassMetaData() {
        GeneratedClassMetaData underTest = new GeneratedClassMetaData(ID, "GeneratedClass", null, null);

        assertThat(underTest.getShortIdentifier()).isEqualTo(GeneratedIdentifier.of(ID));
    }

    @Test
    @FixFor("debezium/dbz#2726")
    @DisplayName("should be used as short identifier by the generated converter class metadata")
    void shouldBeUsedByGeneratedConverterClassMetaData() {
        GeneratedConverterClassMetaData underTest = new GeneratedConverterClassMetaData(ID, "GeneratedConverterClass", null, null);

        assertThat(underTest.getShortIdentifier()).isEqualTo(GeneratedIdentifier.of(ID));
    }
}
