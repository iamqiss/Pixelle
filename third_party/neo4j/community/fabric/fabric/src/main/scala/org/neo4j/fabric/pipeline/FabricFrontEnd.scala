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
package org.neo4j.fabric.pipeline

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.cypher.internal.CachingPreParser
import org.neo4j.cypher.internal.PreParsedQuery
import org.neo4j.cypher.internal.QueryOptions
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.MultipleGraphs
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.UseAsMultipleGraphsSelector
import org.neo4j.cypher.internal.cache.CacheSize
import org.neo4j.cypher.internal.cache.CacheTracer
import org.neo4j.cypher.internal.cache.CaffeineCacheFactory
import org.neo4j.cypher.internal.cache.CypherQueryCaches
import org.neo4j.cypher.internal.cache.CypherQueryCaches.PreParserCache
import org.neo4j.cypher.internal.compiler.helpers.ParameterValueTypeHelper
import org.neo4j.cypher.internal.compiler.phases.BaseContextImpl
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.InternalSyntaxUsageStats
import org.neo4j.cypher.internal.frontend.phases.ScopedProcedureSignatureResolver
import org.neo4j.cypher.internal.options.CypherExecutionMode
import org.neo4j.cypher.internal.planner.spi.PlannerNameFor
import org.neo4j.cypher.internal.planning.WrappedMonitors
import org.neo4j.cypher.internal.tracing.CompilationTracer
import org.neo4j.cypher.internal.tracing.TimingCompilationTracer
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.internal.util.InternalNotificationLogger
import org.neo4j.cypher.rendering.QueryRenderer
import org.neo4j.fabric.planning.FabricPlan
import org.neo4j.fabric.util.Errors
import org.neo4j.kernel.database.DatabaseReference
import org.neo4j.monitoring
import org.neo4j.values.virtual.MapValue

case class FabricFrontEnd(
  cypherConfig: CypherConfiguration,
  kernelMonitors: monitoring.Monitors,
  cacheFactory: CaffeineCacheFactory
) {

  val compilationTracer = new TimingCompilationTracer(
    kernelMonitors.newMonitor(classOf[TimingCompilationTracer.EventListener])
  )

  object preParsing {

    private val preParser = new CachingPreParser(
      cypherConfig,
      new PreParserCache.Cache(
        cacheFactory,
        CacheSize.Dynamic(cypherConfig.queryCacheSize),
        new CacheTracer[CypherQueryCaches.PreParserCache.Key] {}
      )
    )

    def executionType(options: QueryOptions, inCompositeContext: Boolean): FabricPlan.ExecutionType =
      options.queryOptions.executionMode match {
        case CypherExecutionMode.default => FabricPlan.Execute
        case CypherExecutionMode.explain => FabricPlan.Explain
        case CypherExecutionMode.profile if inCompositeContext =>
          Errors.semantic("'PROFILE' is not supported on composite databases.")
        case CypherExecutionMode.profile => FabricPlan.PROFILE
      }

    def preParse(queryString: String, notificationLogger: InternalNotificationLogger): PreParsedQuery = {
      preParser.preParseQuery(queryString, notificationLogger)
    }

  }

  case class Pipeline(
    signatures: ScopedProcedureSignatureResolver,
    query: PreParsedQuery,
    params: MapValue,
    cancellationChecker: CancellationChecker,
    notificationLogger: InternalNotificationLogger,
    internalSyntaxUsageStats: InternalSyntaxUsageStats,
    sessionDatabase: DatabaseReference
  ) {

    def traceStart(): CompilationTracer.QueryCompilationEvent =
      compilationTracer.compileQuery(query.description)

    private val context: BaseContext = BaseContextImpl(
      CompilationPhaseTracer.NO_TRACING,
      notificationLogger,
      query.rawStatement,
      Some(query.options.offset),
      WrappedMonitors(kernelMonitors),
      cancellationChecker,
      internalSyntaxUsageStats,
      sessionDatabase
    )

    private val semanticFeatures = Seq(
      MultipleGraphs,
      UseAsMultipleGraphsSelector
    )

    private val parsingConfig = CompilationPhases.ParsingConfig(
      cypherVersion = query.options.queryOptions.cypherVersion.actualVersion,
      extractLiterals = cypherConfig.extractLiterals,
      parameterTypeMapping = ParameterValueTypeHelper.asCypherTypeMap(params, cypherConfig.useParameterSizeHint),
      semanticFeatures =
        CompilationPhases.enabledSemanticFeatures(
          cypherConfig.enableExtraSemanticFeatures ++ cypherConfig.toggledFeatures(Map(
            GraphDatabaseInternalSettings.show_setting -> SemanticFeature.ShowSetting.productPrefix,
            GraphDatabaseInternalSettings.composable_commands -> SemanticFeature.ComposableCommands.productPrefix,
            GraphDatabaseInternalSettings.graph_type_enabled -> SemanticFeature.GraphTypes.productPrefix
          ))
        ) ++ semanticFeatures,
      obfuscateLiterals = cypherConfig.obfuscateLiterals,
      antlrParserEnabled = cypherConfig.cypherParserAntlrEnabled
    )

    object parseAndPrepare {

      // We need to make sure that names generated in `parseAndPrepare` cannot ever clash with names
      // generated elsewhere. The reason is that a query fragment that gets sent to a remote only undergoes
      // parseAndPrepare. On the remote, a new anonymousVariableNameGenerator will be used and that must never
      // clash with this one.
      private val anonymousVariableNameGenerator = new AnonymousVariableNameGenerator(negativeNumbers = true)

      private val transformer =
        CompilationPhases
          .fabricParsing(parsingConfig, signatures)

      def process(): BaseState =
        transformer.transform(
          InitialState(query.statement, null, anonymousVariableNameGenerator),
          context
        )
    }

    object checkAndFinalize {

      private val anonymousVariableNameGenerator = new AnonymousVariableNameGenerator(negativeNumbers = false)

      private val transformer =
        CompilationPhases.fabricFinalize(parsingConfig)

      def process(statement: Statement, useFullQueryText: Boolean): BaseState = {
        val localQueryString =
          if (useFullQueryText)
            query.rawStatement
          else
            QueryRenderer.render(statement)

        val plannerName = PlannerNameFor(query.options.queryOptions.planner.name)
        val state = InitialState(
          localQueryString,
          plannerName,
          anonymousVariableNameGenerator = anonymousVariableNameGenerator
        )
          .withStatement(statement)
        transformer.transform(state, context)
      }
    }

    def internalNotifications: Set[InternalNotification] =
      context.notificationLogger.notifications
  }
}
