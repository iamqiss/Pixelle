/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.common.annotation.processor;

import org.density.common.SuppressForbidden;
import org.density.common.annotation.InternalApi;
import org.density.test.DensityTestCase;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import javax.tools.Diagnostic;

import static org.density.common.annotation.processor.CompilerSupport.HasDiagnostic.matching;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

@SuppressForbidden(reason = "TemporaryFolder does not support Path-based APIs")
@SuppressWarnings("deprecation")
public class ApiAnnotationProcessorTests extends DensityTestCase implements CompilerSupport {
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    public void testPublicApiMethodArgumentNotAnnotated() {
        final CompilerResult result = compile(folder.getRoot().toPath(), "PublicApiMethodArgumentNotAnnotated.java", "NotAnnotated.java");
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(3));

        assertThat(
            failure.diagnotics(),
            hasItem(
                matching(
                    Diagnostic.Kind.ERROR,
                    containsString(
                        "The element org.density.common.annotation.processor.NotAnnotated is part of the public APIs but is not marked as @PublicApi, @ExperimentalApi or @DeprecatedApi "
                            + "(referenced by org.density.common.annotation.processor.PublicApiMethodArgumentNotAnnotated)"
                    )
                )
            )
        );
    }

    public void testPublicApiMethodArgumentNotAnnotatedGenerics() {
        final CompilerResult result = compile(
            folder.getRoot().toPath(),
            "PublicApiMethodArgumentNotAnnotatedGenerics.java",
            "NotAnnotated.java"
        );
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(3));

        assertThat(
            failure.diagnotics(),
            hasItem(
                matching(
                    Diagnostic.Kind.ERROR,
                    containsString(
                        "The element org.density.common.annotation.processor.NotAnnotated is part of the public APIs but is not marked as @PublicApi, @ExperimentalApi or @DeprecatedApi "
                            + "(referenced by org.density.common.annotation.processor.PublicApiMethodArgumentNotAnnotatedGenerics)"
                    )
                )
            )
        );
    }

    public void testPublicApiMethodThrowsNotAnnotated() {
        final CompilerResult result = compile(
            folder.getRoot().toPath(),
            "PublicApiMethodThrowsNotAnnotated.java",
            "PublicApiAnnotated.java"
        );
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(3));

        assertThat(
            failure.diagnotics(),
            hasItem(
                matching(
                    Diagnostic.Kind.ERROR,
                    containsString(
                        "The element org.density.common.annotation.processor.NotAnnotatedException is part of the public APIs but is not marked as @PublicApi, @ExperimentalApi or @DeprecatedApi "
                            + "(referenced by org.density.common.annotation.processor.PublicApiMethodThrowsNotAnnotated)"
                    )
                )
            )
        );
    }

    public void testPublicApiMethodArgumentNotAnnotatedPackagePrivate() {
        final CompilerResult result = compile(folder.getRoot().toPath(), "PublicApiMethodArgumentNotAnnotatedPackagePrivate.java");
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(4));

        assertThat(
            failure.diagnotics(),
            hasItem(
                matching(
                    Diagnostic.Kind.ERROR,
                    containsString(
                        "The element org.density.common.annotation.processor.NotAnnotatedPackagePrivate is part of the public APIs but does not have public or protected visibility "
                            + "(referenced by org.density.common.annotation.processor.PublicApiMethodArgumentNotAnnotatedPackagePrivate)"
                    )
                )
            )
        );

        assertThat(
            failure.diagnotics(),
            hasItem(
                matching(
                    Diagnostic.Kind.ERROR,
                    containsString(
                        "The element org.density.common.annotation.processor.NotAnnotatedPackagePrivate is part of the public APIs but is not marked as @PublicApi, @ExperimentalApi or @DeprecatedApi "
                            + "(referenced by org.density.common.annotation.processor.PublicApiMethodArgumentNotAnnotatedPackagePrivate)"
                    )
                )
            )
        );
    }

    public void testPublicApiMethodArgumentAnnotatedPackagePrivate() {
        final CompilerResult result = compile(folder.getRoot().toPath(), "PublicApiMethodArgumentAnnotatedPackagePrivate.java");
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(3));

        assertThat(
            failure.diagnotics(),
            hasItem(
                matching(
                    Diagnostic.Kind.ERROR,
                    containsString(
                        "The element org.density.common.annotation.processor.AnnotatedPackagePrivate is part of the public APIs but does not have public or protected visibility "
                            + "(referenced by org.density.common.annotation.processor.PublicApiMethodArgumentAnnotatedPackagePrivate)"
                    )
                )
            )
        );
    }

    public void testPublicApiWithInternalApiMethod() {
        final CompilerResult result = compile(folder.getRoot().toPath(), "PublicApiWithInternalApiMethod.java");
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(3));

        assertThat(
            failure.diagnotics(),
            hasItem(
                matching(
                    Diagnostic.Kind.ERROR,
                    containsString(
                        "The element method() is part of the public APIs but is marked as @InternalApi (referenced by org.density.common.annotation.processor.PublicApiWithInternalApiMethod)"
                    )
                )
            )
        );
    }

    /**
     * The constructor arguments have relaxed semantics at the moment: those could be not annotated or be annotated as {@link InternalApi}
     */
    public void testPublicApiConstructorArgumentNotAnnotated() {
        final CompilerResult result = compile(
            folder.getRoot().toPath(),
            "PublicApiConstructorArgumentNotAnnotated.java",
            "NotAnnotated.java"
        );
        assertThat(result, instanceOf(Success.class));

        final Success success = (Success) result;
        assertThat(success.diagnotics(), hasSize(2));

        assertThat(success.diagnotics(), not(hasItem(matching(Diagnostic.Kind.ERROR))));
    }

    /**
     * The constructor arguments have relaxed semantics at the moment: those could be not annotated or be annotated as {@link InternalApi}
     */
    public void testPublicApiConstructorArgumentAnnotatedInternalApi() {
        final CompilerResult result = compile(
            folder.getRoot().toPath(),
            "PublicApiConstructorArgumentAnnotatedInternalApi.java",
            "InternalApiAnnotated.java"
        );
        assertThat(result, instanceOf(Success.class));

        final Success success = (Success) result;
        assertThat(success.diagnotics(), hasSize(2));

        assertThat(success.diagnotics(), not(hasItem(matching(Diagnostic.Kind.ERROR))));
    }

    public void testPublicApiWithExperimentalApiMethod() {
        final CompilerResult result = compile(folder.getRoot().toPath(), "PublicApiWithExperimentalApiMethod.java");
        assertThat(result, instanceOf(Success.class));

        final Success success = (Success) result;
        assertThat(success.diagnotics(), hasSize(2));

        assertThat(success.diagnotics(), not(hasItem(matching(Diagnostic.Kind.ERROR))));
    }

    public void testPublicApiMethodReturnNotAnnotated() {
        final CompilerResult result = compile(folder.getRoot().toPath(), "PublicApiMethodReturnNotAnnotated.java", "NotAnnotated.java");
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(3));

        assertThat(
            failure.diagnotics(),
            hasItem(
                matching(
                    Diagnostic.Kind.ERROR,
                    containsString(
                        "The element org.density.common.annotation.processor.NotAnnotated is part of the public APIs but is not marked as @PublicApi, @ExperimentalApi or @DeprecatedApi "
                            + "(referenced by org.density.common.annotation.processor.PublicApiMethodReturnNotAnnotated)"
                    )
                )
            )
        );
    }

    public void testPublicApiMethodReturnNotAnnotatedGenerics() {
        final CompilerResult result = compile(
            folder.getRoot().toPath(),
            "PublicApiMethodReturnNotAnnotatedGenerics.java",
            "NotAnnotated.java"
        );
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(3));

        assertThat(
            failure.diagnotics(),
            hasItem(
                matching(
                    Diagnostic.Kind.ERROR,
                    containsString(
                        "The element org.density.common.annotation.processor.NotAnnotated is part of the public APIs but is not marked as @PublicApi, @ExperimentalApi or @DeprecatedApi "
                            + "(referenced by org.density.common.annotation.processor.PublicApiMethodReturnNotAnnotatedGenerics)"
                    )
                )
            )
        );
    }

    public void testPublicApiMethodReturnNotAnnotatedArray() {
        final CompilerResult result = compile(
            folder.getRoot().toPath(),
            "PublicApiMethodReturnNotAnnotatedArray.java",
            "NotAnnotated.java"
        );
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(3));

        assertThat(
            failure.diagnotics(),
            hasItem(
                matching(
                    Diagnostic.Kind.ERROR,
                    containsString(
                        "The element org.density.common.annotation.processor.NotAnnotated is part of the public APIs but is not marked as @PublicApi, @ExperimentalApi or @DeprecatedApi "
                            + "(referenced by org.density.common.annotation.processor.PublicApiMethodReturnNotAnnotatedArray)"
                    )
                )
            )
        );
    }

    public void testPublicApiMethodReturnNotAnnotatedBoundedGenerics() {
        final CompilerResult result = compile(
            folder.getRoot().toPath(),
            "PublicApiMethodReturnNotAnnotatedBoundedGenerics.java",
            "NotAnnotated.java"
        );
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(3));

        assertThat(
            failure.diagnotics(),
            hasItem(
                matching(
                    Diagnostic.Kind.ERROR,
                    containsString(
                        "The element org.density.common.annotation.processor.NotAnnotated is part of the public APIs but is not marked as @PublicApi, @ExperimentalApi or @DeprecatedApi "
                            + "(referenced by org.density.common.annotation.processor.PublicApiMethodReturnNotAnnotatedBoundedGenerics)"
                    )
                )
            )
        );
    }

    public void testPublicApiMethodReturnNotAnnotatedAnnotation() {
        final CompilerResult result = compile(
            folder.getRoot().toPath(),
            "PublicApiMethodReturnNotAnnotatedAnnotation.java",
            "PublicApiAnnotated.java",
            "NotAnnotatedAnnotation.java"
        );
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(3));

        assertThat(
            failure.diagnotics(),
            hasItem(
                matching(
                    Diagnostic.Kind.ERROR,
                    containsString(
                        "The element org.density.common.annotation.processor.NotAnnotatedAnnotation is part of the public APIs but is not marked as @PublicApi, @ExperimentalApi or @DeprecatedApi "
                            + "(referenced by org.density.common.annotation.processor.PublicApiMethodReturnNotAnnotatedAnnotation)"
                    )
                )
            )
        );
    }

    public void testPublicApiMethodReturnNotAnnotatedWildcardGenerics() {
        final CompilerResult result = compile(folder.getRoot().toPath(), "PublicApiMethodReturnNotAnnotatedWildcardGenerics.java");
        assertThat(result, instanceOf(Success.class));

        final Success success = (Success) result;
        assertThat(success.diagnotics(), hasSize(2));

        assertThat(success.diagnotics(), not(hasItem(matching(Diagnostic.Kind.ERROR))));
    }

    public void testPublicApiWithPackagePrivateMethod() {
        final CompilerResult result = compile(folder.getRoot().toPath(), "PublicApiWithPackagePrivateMethod.java", "NotAnnotated.java");
        assertThat(result, instanceOf(Success.class));

        final Success success = (Success) result;
        assertThat(success.diagnotics(), hasSize(2));

        assertThat(success.diagnotics(), not(hasItem(matching(Diagnostic.Kind.ERROR))));
    }

    public void testPublicApiMethodReturnSelf() {
        final CompilerResult result = compile(folder.getRoot().toPath(), "PublicApiMethodReturnSelf.java");
        assertThat(result, instanceOf(Success.class));

        final Success success = (Success) result;
        assertThat(success.diagnotics(), hasSize(2));

        assertThat(success.diagnotics(), not(hasItem(matching(Diagnostic.Kind.ERROR))));
    }

    public void testExperimentalApiMethodReturnSelf() {
        final CompilerResult result = compile(folder.getRoot().toPath(), "ExperimentalApiMethodReturnSelf.java");
        assertThat(result, instanceOf(Success.class));

        final Success success = (Success) result;
        assertThat(success.diagnotics(), hasSize(2));

        assertThat(success.diagnotics(), not(hasItem(matching(Diagnostic.Kind.ERROR))));
    }

    public void testDeprecatedApiMethodReturnSelf() {
        final CompilerResult result = compile(folder.getRoot().toPath(), "DeprecatedApiMethodReturnSelf.java");
        assertThat(result, instanceOf(Success.class));

        final Success success = (Success) result;
        assertThat(success.diagnotics(), hasSize(2));

        assertThat(success.diagnotics(), not(hasItem(matching(Diagnostic.Kind.ERROR))));
    }

    public void testPublicApiPackagePrivate() {
        final CompilerResult result = compile(folder.getRoot().toPath(), "PublicApiPackagePrivate.java");
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(3));

        assertThat(
            failure.diagnotics(),
            hasItem(
                matching(
                    Diagnostic.Kind.ERROR,
                    containsString(
                        "The element org.density.common.annotation.processor.PublicApiPackagePrivate is part of the public APIs but does not have public or protected visibility"
                    )
                )
            )
        );
    }

    public void testPublicApiMethodGenericsArgumentNotAnnotated() {
        final CompilerResult result = compile(
            folder.getRoot().toPath(),
            "PublicApiMethodGenericsArgumentNotAnnotated.java",
            "NotAnnotated.java"
        );
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(3));

        assertThat(
            failure.diagnotics(),
            hasItem(
                matching(
                    Diagnostic.Kind.ERROR,
                    containsString(
                        "The element org.density.common.annotation.processor.NotAnnotated is part of the public APIs but is not marked as @PublicApi, @ExperimentalApi or @DeprecatedApi "
                            + "(referenced by org.density.common.annotation.processor.PublicApiMethodGenericsArgumentNotAnnotated)"
                    )
                )
            )
        );
    }

    public void testPublicApiMethodReturnAnnotatedArray() {
        final CompilerResult result = compile(
            folder.getRoot().toPath(),
            "PublicApiMethodReturnAnnotatedArray.java",
            "PublicApiAnnotated.java"
        );
        assertThat(result, instanceOf(Success.class));

        final Success success = (Success) result;
        assertThat(success.diagnotics(), hasSize(2));

        assertThat(success.diagnotics(), not(hasItem(matching(Diagnostic.Kind.ERROR))));
    }

    public void testPublicApiMethodGenericsArgumentAnnotated() {
        final CompilerResult result = compile(
            folder.getRoot().toPath(),
            "PublicApiMethodGenericsArgumentAnnotated.java",
            "PublicApiAnnotated.java"
        );
        assertThat(result, instanceOf(Success.class));

        final Success success = (Success) result;
        assertThat(success.diagnotics(), hasSize(2));

        assertThat(success.diagnotics(), not(hasItem(matching(Diagnostic.Kind.ERROR))));
    }

    public void testPublicApiAnnotatedNotOpensearch() {
        final CompilerResult result = compileWithPackage(folder.getRoot().toPath(), "org.acme", "PublicApiAnnotated.java");
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(3));

        assertThat(
            failure.diagnotics(),
            hasItem(
                matching(
                    Diagnostic.Kind.ERROR,
                    containsString(
                        "The type org.acme.PublicApiAnnotated is not residing in org.density.* package and should not be annotated as Density APIs."
                    )
                )
            )
        );
    }

    public void testPublicApiMethodReturnAnnotatedGenerics() {
        final CompilerResult result = compile(
            folder.getRoot().toPath(),
            "PublicApiMethodReturnAnnotatedGenerics.java",
            "PublicApiAnnotated.java",
            "NotAnnotatedAnnotation.java"
        );
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(3));

        assertThat(
            failure.diagnotics(),
            hasItem(
                matching(
                    Diagnostic.Kind.ERROR,
                    containsString(
                        "The element org.density.common.annotation.processor.NotAnnotatedAnnotation is part of the public APIs but is not marked as @PublicApi, @ExperimentalApi or @DeprecatedApi "
                            + "(referenced by org.density.common.annotation.processor.PublicApiMethodReturnAnnotatedGenerics)"
                    )
                )
            )
        );
    }

    /**
     * The type could expose protected inner types which are still considered to be a public API when used
     */
    public void testPublicApiWithProtectedInterface() {
        final CompilerResult result = compile(folder.getRoot().toPath(), "PublicApiWithProtectedInterface.java");
        assertThat(result, instanceOf(Success.class));

        final Success success = (Success) result;
        assertThat(success.diagnotics(), hasSize(2));

        assertThat(success.diagnotics(), not(hasItem(matching(Diagnostic.Kind.ERROR))));
    }

    /**
     * The constructor arguments have relaxed semantics at the moment: those could be not annotated or be annotated as {@link InternalApi}
     */
    public void testPublicApiConstructorAnnotatedInternalApi() {
        final CompilerResult result = compile(
            folder.getRoot().toPath(),
            "PublicApiConstructorAnnotatedInternalApi.java",
            "NotAnnotated.java"
        );
        assertThat(result, instanceOf(Success.class));

        final Success success = (Success) result;
        assertThat(success.diagnotics(), hasSize(2));

        assertThat(success.diagnotics(), not(hasItem(matching(Diagnostic.Kind.ERROR))));
    }

    public void testPublicApiUnparseableVersion() {
        final CompilerResult result = compile(folder.getRoot().toPath(), "PublicApiAnnotatedUnparseable.java");
        assertThat(result, instanceOf(Failure.class));

        final Failure failure = (Failure) result;
        assertThat(failure.diagnotics(), hasSize(3));

        assertThat(
            failure.diagnotics(),
            hasItem(
                matching(
                    Diagnostic.Kind.ERROR,
                    containsString(
                        "The type org.density.common.annotation.processor.PublicApiAnnotatedUnparseable has @PublicApi annotation with unparseable Density version: 2.x"
                    )
                )
            )
        );
    }

    public void testPublicApiWithDeprecatedApiMethod() {
        final CompilerResult result = compile(folder.getRoot().toPath(), "PublicApiWithDeprecatedApiMethod.java");
        assertThat(result, instanceOf(Success.class));

        final Success success = (Success) result;
        assertThat(success.diagnotics(), hasSize(2));

        assertThat(success.diagnotics(), not(hasItem(matching(Diagnostic.Kind.ERROR))));
    }

}
