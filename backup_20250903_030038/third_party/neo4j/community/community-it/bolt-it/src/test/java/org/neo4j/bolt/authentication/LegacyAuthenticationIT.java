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
package org.neo4j.bolt.authentication;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.logging.AssertableLogProvider.Level.WARN;
import static org.neo4j.test.assertion.Assert.awaitUntilAsserted;
import static org.neo4j.test.conditions.Conditions.TRUE;

import java.util.List;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.bolt.protocol.common.connector.connection.AtomicSchedulingConnection;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.VersionSelected;
import org.neo4j.bolt.test.annotation.setup.FactoryFunction;
import org.neo4j.bolt.test.annotation.setup.SettingsFunction;
import org.neo4j.bolt.test.annotation.test.ProtocolTest;
import org.neo4j.bolt.test.annotation.wire.selector.IncludeWire;
import org.neo4j.bolt.test.provider.ConnectionProvider;
import org.neo4j.bolt.testing.annotation.Version;
import org.neo4j.bolt.testing.assertions.BoltConnectionAssertions;
import org.neo4j.bolt.testing.client.BoltTestConnection;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.assertion.Assert;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

/**
 * Ensures that authentication is processed, returns the correct metadata and failures.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
@IncludeWire({@Version(major = 4), @Version(major = 5, minor = 0)})
public class LegacyAuthenticationIT {

    protected final AssertableLogProvider userLogProvider = new AssertableLogProvider();

    @FactoryFunction
    protected void customizeDatabase(TestDatabaseManagementServiceBuilder factory) {
        factory.setUserLogProvider(this.userLogProvider);
    }

    @SettingsFunction
    protected void customizeSettings(Map<Setting<?>, Object> settings) {
        settings.put(GraphDatabaseSettings.auth_enabled, true);
    }

    @AfterEach
    void cleanup() {
        this.userLogProvider.clear();
    }

    private static MapValue singletonMap(String key, Object value) {
        return VirtualValues.map(new String[] {key}, new AnyValue[] {ValueUtils.of(value)});
    }

    @ProtocolTest
    void shouldRespondWithCredentialsExpiredOnFirstUse(BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello(x -> x.withBasicAuth("neo4j", "neo4j")));

        // ensure that the server returns the expected set of metadata as well as a marker indicating that the used
        // credentials have expired and will need to be changed
        BoltConnectionAssertions.assertThat(connection).receivesSuccess(meta -> Assertions.assertThat(meta)
                .containsKeys("server", "connection_id")
                .containsEntry("credentials_expired", true));
    }

    @ProtocolTest
    void shouldFailIfWrongCredentials(BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello(x -> x.withBasicAuth("neo4j", "wrong")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailureV40(
                        Status.Security.Unauthorized, "The client is unauthorized due to authentication failure.")
                .isEventuallyTerminated();

        Assert.assertEventually(
                () -> "Matching log call not found in\n" + this.userLogProvider.serialize(),
                () -> {
                    try {
                        LogAssertions.assertThat(this.userLogProvider)
                                .forClass(AtomicSchedulingConnection.class)
                                .forLevel(WARN)
                                .containsMessages("The client is unauthorized due to authentication failure.");
                        return true;
                    } catch (AssertionError e) {
                        return false;
                    }
                },
                TRUE,
                30,
                SECONDS);
    }

    @ProtocolTest
    void shouldFailIfWrongCredentialsFollowingSuccessfulLogin(
            BoltWire wire, @VersionSelected ConnectionProvider connectionProvider) {
        try (var connection = connectionProvider.create()) {
            // authenticate normally using the preset credentials and update the password to a new value
            connection.send(wire.hello(x -> x.withBasicAuth("neo4j", "neo4j")));

            BoltConnectionAssertions.assertThat(connection).receivesSuccess();

            connection.send(wire.run("ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password", x -> x.withParameters(
                            singletonMap("password", "secretPassword"))
                    .withDatabase(SYSTEM_DATABASE_NAME)));
            connection.send(wire.pull());

            BoltConnectionAssertions.assertThat(connection).receivesSuccess(2);
        }

        // attempt to authenticate again with the new password
        try (var connection = connectionProvider.create()) {
            connection.send(wire.hello(x -> x.withBasicAuth("neo4j", "secretPassword")));

            BoltConnectionAssertions.assertThat(connection).receivesSuccess();
        }

        // attempt to authenticate again with the old password
        try (var connection = connectionProvider.create()) {
            connection.send(wire.hello(x -> x.withBasicAuth("neo4j", "neo4j")));

            BoltConnectionAssertions.assertThat(connection)
                    .receivesFailureV40(
                            Status.Security.Unauthorized, "The client is unauthorized due to authentication failure.")
                    .isEventuallyTerminated();
        }
    }

    @ProtocolTest
    void shouldFailIfMalformedAuthTokenWrongType(BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello(
                x -> x.withBasicScheme().withBadPrincipal(List.of("neo4j")).withCredentials("neo4j")));
        BoltConnectionAssertions.assertThat(connection)
                .receivesFailureFuzzyV40(
                        Status.Security.Unauthorized,
                        "Unsupported authentication token, the value associated with the key `principal` must be a String but was: ArrayList")
                .isEventuallyTerminated();
    }

    @ProtocolTest
    void shouldFailIfMalformedAuthTokenMissingKey(BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello(x -> x.withBasicScheme()
                .withPrincipal("neo4j")
                .withBadKeyPair("this-should-have-been-credentials", "neo4j")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailureFuzzyV40(
                        Status.Security.Unauthorized, "Unsupported authentication token, missing key `credentials`")
                .isEventuallyTerminated();
    }

    @ProtocolTest
    void shouldFailIfMalformedAuthTokenMissingScheme(BoltWire wire, @VersionSelected BoltTestConnection connection) {

        connection.send(wire.hello(x -> x.withPrincipal("neo4j").withCredentials("neo4j")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailureFuzzyV40(
                        Status.Security.Unauthorized, "Unsupported authentication token, missing key `scheme`")
                .isEventuallyTerminated();
    }

    @ProtocolTest
    protected void shouldFailIfMalformedAuthTokenUnknownScheme(
            BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(
                wire.hello(x -> x.withScheme("unknown").withPrincipal("neo4j").withCredentials("neo4j")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailureV40(
                        Status.Security.Unauthorized,
                        "Unsupported authentication token, scheme 'unknown' is not supported.")
                .isEventuallyTerminated();
    }

    @ProtocolTest
    void shouldFailDifferentlyIfTooManyFailedAuthAttempts(
            BoltWire wire, @VersionSelected ConnectionProvider connectionProvider) {
        awaitUntilAsserted(() -> {
            try (var connection = connectionProvider.create()) {
                connection.send(wire.hello(x -> x.withBasicAuth("neo4j", "WHAT_WAS_THE_PASSWORD_AGAIN")));

                BoltConnectionAssertions.assertThat(connection)
                        .receivesFailureV40(
                                Status.Security.AuthenticationRateLimit,
                                "The client has provided incorrect authentication details too many times in a row.")
                        .isEventuallyTerminated();
            }
        });
    }

    @ProtocolTest
    void shouldFailWhenReusingTheSamePassword(BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello(x -> x.withBasicAuth("neo4j", "neo4j")));

        BoltConnectionAssertions.assertThat(connection).receivesSuccess(meta -> Assertions.assertThat(meta)
                .containsEntry("credentials_expired", true)
                .containsKeys("server", "connection_id"));

        connection
                .send(wire.reset())
                .send(wire.run("ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password", x -> x.withParameters(
                                singletonMap("password", "password"))
                        .withDatabase(SYSTEM_DATABASE_NAME)))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection).receivesSuccess(3);

        connection
                .send(wire.run("ALTER CURRENT USER SET PASSWORD FROM 'password' TO $password", x -> x.withParameters(
                                singletonMap("password", "password"))
                        .withDatabase(SYSTEM_DATABASE_NAME)))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailureFuzzyV40(
                        Status.Statement.ArgumentError, "Old password and new password cannot be the same.")
                .receivesIgnored();

        connection
                .send(wire.reset())
                .send(wire.run("ALTER CURRENT USER SET PASSWORD FROM 'password' TO $password", x -> x.withParameters(
                                singletonMap("password", "abcdefgh"))
                        .withDatabase(SYSTEM_DATABASE_NAME)))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection).receivesSuccess(3);
    }

    @ProtocolTest
    void shouldFailWhenSubmittingEmptyPassword(BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello(x -> x.withBasicAuth("neo4j", "neo4j")));

        BoltConnectionAssertions.assertThat(connection).receivesSuccess(meta -> Assertions.assertThat(meta)
                .containsEntry("credentials_expired", true)
                .containsKeys("server", "connection_id"));

        connection
                .send(wire.run(
                        "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password",
                        x -> x.withParameters(singletonMap("password", "")).withDatabase(SYSTEM_DATABASE_NAME)))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailureV40(Status.Statement.ArgumentError, "A password cannot be empty.")
                .receivesIgnored();

        connection
                .send(wire.reset())
                .send(wire.run("ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password", x -> x.withParameters(
                                singletonMap("password", "abcdefgh"))
                        .withDatabase(SYSTEM_DATABASE_NAME)))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection).receivesSuccess(3);
    }

    @ProtocolTest
    void shouldNotBeAbleToReadWhenPasswordChangeRequired(
            BoltWire wire, @VersionSelected BoltTestConnection connection) {
        // authenticate with the default (expired) credentials
        connection.send(wire.hello(x -> x.withBasicAuth("neo4j", "neo4j")));

        BoltConnectionAssertions.assertThat(connection).receivesSuccess(meta -> Assertions.assertThat(meta)
                .containsEntry("credentials_expired", true)
                .containsKeys("server", "connection_id"));

        // attempt to execute a query
        connection.send(wire.run("MATCH (n) RETURN n")).send(wire.pull());

        // which should fail with one of two possible errors
        try {
            BoltConnectionAssertions.assertThat(connection)
                    .receivesFailureFuzzyV40(
                            Status.Security.CredentialsExpired,
                            "The credentials you provided were valid, but must be changed before you can use this instance.");
        } catch (AssertionError ignore) {
            // Compiled runtime triggers the AuthorizationViolation exception on the PULL_N message, which means the RUN
            // message will
            // give a Success response. This should not matter much since RUN + PULL_N are always sent together.
            BoltConnectionAssertions.assertThat(connection)
                    .receivesFailureFuzzyV40(
                            Status.Security.CredentialsExpired,
                            "The credentials you provided were valid, but must be changed before you can use this instance.");
        }
    }
}
