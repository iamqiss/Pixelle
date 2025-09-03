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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager

import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.ConflictFinder.ConflictingPlanPair
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.EagerWhereNeededRewriter.PlanChildrenLookup
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.ReadsAndWritesFinder.FilterExpressions
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.ReadsAndWritesFinder.PlanThatIntroducesVariable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.ReadsAndWritesFinder.PlanWithAccessor
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.ReadsAndWritesFinder.PossibleDeleteConflictPlans
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.ReadsAndWritesFinder.ReadsAndWrites
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.ir.EagernessReason.Conflict
import org.neo4j.cypher.internal.ir.EagernessReason.LabelReadRemoveConflict
import org.neo4j.cypher.internal.ir.EagernessReason.LabelReadSetConflict
import org.neo4j.cypher.internal.ir.EagernessReason.PropertyReadSetConflict
import org.neo4j.cypher.internal.ir.EagernessReason.ReadCreateConflict
import org.neo4j.cypher.internal.ir.EagernessReason.ReadDeleteConflict
import org.neo4j.cypher.internal.ir.EagernessReason.TypeReadSetConflict
import org.neo4j.cypher.internal.ir.EagernessReason.UnknownLabelReadRemoveConflict
import org.neo4j.cypher.internal.ir.EagernessReason.UnknownLabelReadSetConflict
import org.neo4j.cypher.internal.ir.EagernessReason.UnknownPropertyReadSetConflict
import org.neo4j.cypher.internal.ir.RemoveLabelPattern
import org.neo4j.cypher.internal.ir.helpers.CachedFunction
import org.neo4j.cypher.internal.ir.helpers.overlaps.CreateOverlaps
import org.neo4j.cypher.internal.ir.helpers.overlaps.CreateOverlaps.NodeLabelsOverlap
import org.neo4j.cypher.internal.ir.helpers.overlaps.CreateOverlaps.PropertiesOverlap
import org.neo4j.cypher.internal.ir.helpers.overlaps.CreateOverlaps.RelationshipTypeOverlap
import org.neo4j.cypher.internal.ir.helpers.overlaps.DeleteOverlaps
import org.neo4j.cypher.internal.ir.helpers.overlaps.Expressions
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.DeleteNode
import org.neo4j.cypher.internal.logical.plans.DeleteRelationship
import org.neo4j.cypher.internal.logical.plans.DetachDeleteNode
import org.neo4j.cypher.internal.logical.plans.Foreach
import org.neo4j.cypher.internal.logical.plans.ForeachApply
import org.neo4j.cypher.internal.logical.plans.LogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.Merge
import org.neo4j.cypher.internal.logical.plans.NestedPlanExpression
import org.neo4j.cypher.internal.logical.plans.NodeLogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.RelationshipLogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.RemoveLabels
import org.neo4j.cypher.internal.logical.plans.StableLeafPlan
import org.neo4j.cypher.internal.logical.plans.UpdatingPlan
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Ref
import org.neo4j.cypher.internal.util.helpers.MapSupport.PowerMap

import scala.collection.View
import scala.collection.mutable
import scala.util.hashing.MurmurHash3

/**
 * Finds conflicts between plans that need Eager to solve them.
 */
sealed trait ConflictFinder {

  // Use Set.empty.incl instead of the generic apply method, for performance.
  private def set1[N](elem: N): Set[N] = Set.empty.incl(elem)

  private def propertyConflicts(
    readsAndWrites: ReadsAndWrites,
    leftMostLeaf: LogicalPlan,
    writtenProperties: ReadsAndWritesFinder.Sets => Iterator[(Option[PropertyKeyName], Set[PlanWithAccessor])],
    plansReadingProperty: (ReadsAndWritesFinder.Reads, Option[PropertyKeyName]) => Iterator[PlanWithAccessor]
  )(implicit planChildrenLookup: PlanChildrenLookup): Iterator[ConflictingPlanPair] = {
    for {
      (prop, writePlans) <- writtenProperties(readsAndWrites.writes.sets)
      read @ PlanWithAccessor(Ref(readPlan), _) <- plansReadingProperty(readsAndWrites.reads, prop)
      write @ PlanWithAccessor(Ref(writePlan), _) <- writePlans.iterator

      conflictType = planChildrenLookup.mostDownstreamPlan(readPlan, writePlan) match {
        case `readPlan` => WriteReadConflict
        case _          => ReadWriteConflict
      }

      // Potentially discard distinct Read -> Write conflicts (keep non ReadWriteConflicts)
      if conflictType != ReadWriteConflict ||
        !distinctConflictOnSameSymbol(read, write) ||
        // For property conflicts, we cannot disregard all conflicts with distinct reads on the same variable.
        // We have to keep conflicts between property reads from leaf plans and writes in TransactionalApply.
        // That is because changing the property of a node `n` and committing these changes might put `n`
        // at a different position in the index that is being traversed by the read, so that `n` could potentially
        // be encountered again.
        (planChildrenLookup.isInTransactionalApply(writePlan) && readPlan.isInstanceOf[LogicalLeafPlan])

      // Discard distinct Write -> Read conflicts (keep non WriteReadConflict)
      if conflictType != WriteReadConflict ||
        // invoke distinctConflictOnSameSymbol with swapped read and write to check that the write is unique.
        !distinctConflictOnSameSymbol(write, read)

      if isValidConflict(readPlan, writePlan, leftMostLeaf)
    } yield {
      val conflict = Conflict(writePlan.id, readPlan.id)
      val reasons = set1[EagernessReason](prop.map(PropertyReadSetConflict(_).withConflict(conflict))
        .getOrElse(UnknownPropertyReadSetConflict.withConflict(conflict)))

      ConflictingPlanPair(Ref(writePlan), Ref(readPlan), reasons)
    }
  }

  private def allWrittenLabels(readsAndWrites: ReadsAndWrites): Iterator[(Option[LabelName], Set[PlanWithAccessor])] = {
    readsAndWrites.writes.sets.writtenLabels.iterator.map {
      case (labelName, plansWithAccessors) => (Some(labelName), plansWithAccessors)
    } ++ Seq((Option.empty, readsAndWrites.writes.sets.writtenUnknownLabels))
  }

  private def labelConflicts(readsAndWrites: ReadsAndWrites, leftMostLeaf: LogicalPlan)(implicit
  planChildrenLookup: PlanChildrenLookup): Iterator[ConflictingPlanPair] = {
    for {
      (maybeLabel, writePlans) <- allWrittenLabels(readsAndWrites)
      read @ PlanWithAccessor(Ref(readPlan), _) <- readsAndWrites.reads.plansReadingLabel(maybeLabel)
      write @ PlanWithAccessor(Ref(writePlan), _) <- writePlans.iterator
      if !distinctConflictOnSameSymbol(read, write)
      if isValidConflict(readPlan, writePlan, leftMostLeaf)
    } yield {
      val conflict = Conflict(writePlan.id, readPlan.id)
      writePlan match {
        case _: RemoveLabels =>
          ConflictingPlanPair(
            Ref(writePlan),
            Ref(readPlan),
            maybeLabel.map(label =>
              set1(LabelReadRemoveConflict(label).withConflict(conflict))
            ).getOrElse(
              set1(UnknownLabelReadRemoveConflict.withConflict(conflict))
            ).toSet
          )
        case forEach: Foreach if forEach.mutations.exists(_.isInstanceOf[RemoveLabelPattern]) =>
          ConflictingPlanPair(
            Ref(writePlan),
            Ref(readPlan),
            maybeLabel.map(label =>
              set1(LabelReadRemoveConflict(label).withConflict(conflict))
            ).getOrElse(
              set1(UnknownLabelReadRemoveConflict.withConflict(conflict))
            ).toSet
          )
        case _ =>
          ConflictingPlanPair(
            Ref(writePlan),
            Ref(readPlan),
            maybeLabel.map(label =>
              set1(LabelReadSetConflict(maybeLabel.get).withConflict(conflict))
            ).getOrElse(
              set1(UnknownLabelReadSetConflict.withConflict(conflict))
            ).toSet
          )
      }
    }
  }

  private def canConflictWithCreateOrDelete(lp: LogicalPlan): Boolean = {
    !lp.isUpdatingPlan || containsNestedPlanExpression(lp)
  }

  protected[eager] def containsNestedPlanExpression(lp: LogicalPlan): Boolean = {
    lp.folder.treeFold(false) {
      case _: NestedPlanExpression => _ => SkipChildren(true)
      // We do not want to find NestedPlanExpressions in child plans.
      case otherLP: LogicalPlan if otherLP ne lp => acc => SkipChildren(acc)
    }
  }

  private def createNodeConflicts(
    readsAndWrites: ReadsAndWrites,
    leftMostLeaf: LogicalPlan
  )(implicit planChildrenLookup: PlanChildrenLookup): Iterator[ConflictingPlanPair] =
    for {
      (Ref(writePlan), createdNodes) <- readsAndWrites.writes.creates.createdNodes.iterator

      // If a variable exists in the snapshot, let's take it from there. This is when we have a read-write conflict.
      // But we have to include other filterExpressions that are not in the snapshot, to also cover write-read conflicts.
      filterExpressions =
        readsAndWrites.reads.nodeFilterExpressions ++
          readsAndWrites.writes.creates.nodeFilterExpressionsSnapshots(Ref(writePlan))

      (readPlans, expressionsDependantOnlyOnVariable) <- filterExpressionsThatCanConflict(filterExpressions)

      createdNode <- createdNodes.iterator

      overlap <-
        CreateOverlaps.findNodeOverlap(
          expressionsDependantOnlyOnVariable,
          createdNode.createdLabels,
          createdNode.createdProperties
        ).iterator

      Ref(readPlan) <- readPlans.iterator
      if isValidConflict(readPlan, writePlan, leftMostLeaf)
    } yield conflictingPlanPair(writePlan, readPlan, overlap)

  private def createRelationshipConflicts(
    readsAndWrites: ReadsAndWrites,
    leftMostLeaf: LogicalPlan
  )(implicit planChildrenLookup: PlanChildrenLookup): Iterator[ConflictingPlanPair] =
    for {
      (Ref(writePlan), createdRelationships) <- readsAndWrites.writes.creates.createdRelationships.iterator

      // If a variable exists in the snapshot, let's take it from there. This is when we have a read-write conflict.
      // But we have to include other filterExpressions that are not in the snapshot, to also cover write-read conflicts.
      filterExpressions =
        readsAndWrites.reads.relationshipFilterExpressions ++
          readsAndWrites.writes.creates.relationshipFilterExpressionsSnapshots(Ref(writePlan))

      (readPlans, expressionsDependantOnlyOnVariable) <- filterExpressionsThatCanConflict(filterExpressions)

      createdRelationship <- createdRelationships.iterator

      overlap <-
        CreateOverlaps.findRelationshipOverlap(
          expressionsDependantOnlyOnVariable,
          createdRelationship.createdType,
          createdRelationship.createdProperties
        ).iterator

      Ref(readPlan) <- readPlans.iterator
      if isValidConflict(readPlan, writePlan, leftMostLeaf)
    } yield conflictingPlanPair(writePlan, readPlan, overlap)

  private def filterExpressionsThatCanConflict(
    filterExpressionsPerVariable: Map[LogicalVariable, FilterExpressions]
  ): Iterator[(Set[Ref[LogicalPlan]], List[Expression])] =
    filterExpressionsPerVariable.iterator.flatMap {
      case (variable, filterExpressions) =>
        val readPlans =
          filterExpressions
            .plansThatIntroduceVariable
            .filter(ref => canConflictWithCreateOrDelete(ref.value))
        if (readPlans.isEmpty)
          Iterator.empty
        else {
          // We need to split the expression in order to filter single predicates.
          // We only want to keep the predicates that depend on only variable, since that is a requirement of CreateOverlaps.overlap
          val expressionsDependantOnlyOnVariable =
            Expressions
              .splitExpression(filterExpressions.expression)
              .filter(_.dependencies == set1(variable))
          Iterator.single((readPlans, expressionsDependantOnlyOnVariable))
        }
    }

  private def conflictingPlanPair(
    writePlan: LogicalPlan,
    readPlan: LogicalPlan,
    overlap: CreateOverlaps.EntityOverlap
  ): ConflictingPlanPair = {
    val labelOrRelTypeOverlapEagernessReasons = overlap match {
      case nodeOverlap: CreateOverlaps.NodeOverlap =>
        nodeOverlap.nodeLabelsOverlap match {
          case NodeLabelsOverlap.Static(labelNames) =>
            labelNames.view.map(LabelReadSetConflict)
          case NodeLabelsOverlap.Dynamic =>
            View.empty
        }
      case relationshipOverlap: CreateOverlaps.RelationshipOverlap =>
        relationshipOverlap.relationshipTypeOverlap match {
          case RelationshipTypeOverlap.Static(relationshipTypeName) =>
            new View.Single(TypeReadSetConflict(relationshipTypeName))
          case RelationshipTypeOverlap.Dynamic =>
            View.empty
        }
    }
    val propertyOverlapEagernessReasons = overlap.propertiesOverlap match {
      case PropertiesOverlap.Overlap(properties) =>
        properties.view.map(property => PropertyReadSetConflict(property))
      case PropertiesOverlap.UnknownOverlap =>
        new View.Single(UnknownPropertyReadSetConflict)
    }
    val combinedEagernessReasons = labelOrRelTypeOverlapEagernessReasons ++ propertyOverlapEagernessReasons
    val eagernessReasons =
      if (combinedEagernessReasons.isEmpty)
        new View.Single(ReadCreateConflict)
      else
        combinedEagernessReasons
    val conflict = Conflict(writePlan.id, readPlan.id)
    val eagernessReasonsWithConflict = eagernessReasons.map(_.withConflict(conflict))
    ConflictingPlanPair(Ref(writePlan), Ref(readPlan), eagernessReasonsWithConflict.toSet)
  }

  /**
   * The read variables for variable DELETE conflicts
   * 
   *  @return A map. For each variable, the possible read plans that can conflict with a given delete (`writePlan`).
   *          Those are split into plansThatIntroduceVariable and plansThatReferenceVariable 
   *          (see [[PossibleDeleteConflictPlans]]).
   *          
   *          Also return the conflict type for each variable.
   */
  private def deleteReadVariables(
    readsAndWrites: ReadsAndWrites,
    writePlan: LogicalPlan,
    possibleDeleteConflictPlans: ReadsAndWritesFinder.Reads => Map[LogicalVariable, PossibleDeleteConflictPlans],
    possibleDeleteConflictPlanSnapshots: (
      ReadsAndWritesFinder.Deletes,
      Ref[LogicalPlan]
    ) => Map[LogicalVariable, PossibleDeleteConflictPlans]
  ): Map[LogicalVariable, (PossibleDeleteConflictPlans, ConflictType)] = {
    // All plans that access the variable.
    val all = possibleDeleteConflictPlans(readsAndWrites.reads)
      .view.mapValues(x => (x, WriteReadConflict)).toMap
    // In the snapshot, we will find only plans that access the variable that are upstream from the write plan.
    val snapshot: Map[LogicalVariable, (PossibleDeleteConflictPlans, ConflictType)] =
      possibleDeleteConflictPlanSnapshots(readsAndWrites.writes.deletes, Ref(writePlan))
        .view.mapValues[(PossibleDeleteConflictPlans, ConflictType)](x => (x, ReadWriteConflict)).toMap

    // If a variable only exists in `all` it is a WriteReadConflict, which we return unchanged here.
    // A variable cannot _only_ exist in `snapshot`.
    snapshot.fuse(all) {
      // If a variable exists in `all` and `snapshot` it is a ReadWriteConflict. We return all plans.
      case ((_, conflictType), (allPlans, _)) => (allPlans, conflictType)
    }
  }

  private def deleteVariableConflicts(
    readsAndWrites: ReadsAndWrites,
    leftMostLeaf: LogicalPlan,
    deletedEntities: ReadsAndWritesFinder.Deletes => Map[Ref[LogicalPlan], Set[Variable]],
    filterExpressions: ReadsAndWritesFinder.Reads => Map[LogicalVariable, FilterExpressions],
    possibleDeleteConflictPlans: ReadsAndWritesFinder.Reads => Map[LogicalVariable, PossibleDeleteConflictPlans],
    possibleDeleteConflictPlanSnapshots: (
      ReadsAndWritesFinder.Deletes,
      Ref[LogicalPlan]
    ) => Map[LogicalVariable, PossibleDeleteConflictPlans]
  )(implicit planChildrenLookup: PlanChildrenLookup): Iterator[ConflictingPlanPair] = {
    for {
      (wpref @ Ref(writePlan), deletedEntities) <- deletedEntities(readsAndWrites.writes.deletes).iterator

      (variable, (PossibleDeleteConflictPlans(plansThatIntroduceVar, plansThatReferenceVariable), conflictType)) <-
        deleteReadVariables(
          readsAndWrites,
          writePlan,
          possibleDeleteConflictPlans,
          possibleDeleteConflictPlanSnapshots
        ).iterator
      if plansThatIntroduceVar.nonEmpty

      // Filter out direct Delete vs Create conflicts
      readPlans = plansThatIntroduceVar.filter(ptiv => canConflictWithCreateOrDelete(ptiv.plan.value))
      if readPlans.nonEmpty ||
        plansThatReferenceVariable // might still have other conflicts left
          .excl(wpref)
          .removedAll(plansThatIntroduceVar.map(_.plan))
          .nonEmpty

      deletedEntity <- deletedEntities.iterator

      FilterExpressions(_, deletedExpression) =
        filterExpressions(readsAndWrites.reads).getOrElse(deletedEntity, FilterExpressions(Set.empty))
      if deleteOverlaps(plansThatIntroduceVar, Seq(deletedExpression))

      // For a ReadWriteConflict we need to place the Eager between the plans that reference the variable and the Delete plan.
      // For a WriteReadConflict we need to place the Eager between the Delete plan and the plan that introduced the variable.
      rpRef @ Ref(readPlan) <- conflictType match {
        case ReadWriteConflict => plansThatReferenceVariable.iterator
        case WriteReadConflict => readPlans.iterator.map(_.plan)
      }
      // For delete, we can only disregard ReadWriteConflicts. We therefore have to keep all WriteReadConflicts.
      if conflictType == WriteReadConflict || !distinctConflictOnSameSymbol(
        PlanWithAccessor(rpRef, Some(variable)),
        PlanWithAccessor(wpref, Some(deletedEntity))
      )
      if isValidConflict(readPlan, writePlan, leftMostLeaf)
    } yield {
      deleteConflict(variable, readPlan, writePlan)
    }
  }

  private def deleteExpressionConflicts(
    readsAndWrites: ReadsAndWrites,
    leftMostLeaf: LogicalPlan,
    deleteExpressions: ReadsAndWritesFinder.Deletes => Iterator[Ref[LogicalPlan]],
    possibleDeleteConflictPlans: ReadsAndWritesFinder.Reads => Map[LogicalVariable, PossibleDeleteConflictPlans],
    possibleDeleteConflictPlanSnapshots: (
      ReadsAndWritesFinder.Deletes,
      Ref[LogicalPlan]
    ) => Map[LogicalVariable, PossibleDeleteConflictPlans]
  )(implicit planChildrenLookup: PlanChildrenLookup): Iterator[ConflictingPlanPair] = {
    for {
      Ref(writePlan) <-
        deleteExpressions(
          readsAndWrites.writes.deletes
        ) ++ readsAndWrites.writes.deletes.plansThatDeleteUnknownTypeExpressions.iterator

      (variable, (PossibleDeleteConflictPlans(plansThatIntroduceVar, plansThatReferenceVariable), conflictType)) <-
        deleteReadVariables(
          readsAndWrites,
          writePlan,
          possibleDeleteConflictPlans,
          possibleDeleteConflictPlanSnapshots
        ).iterator

      // For a ReadWriteConflict we need to place the Eager between the plans that reference the variable and the Delete plan.
      // For a WriteReadConflict we need to place the Eager between the Delete plan and the plan that introduced the variable.
      Ref(readPlan) <- conflictType match {
        case ReadWriteConflict => plansThatReferenceVariable.iterator
        case WriteReadConflict => plansThatIntroduceVar.iterator.map(_.plan)
      }
      if isValidConflict(readPlan, writePlan, leftMostLeaf)
    } yield {
      deleteConflict(variable, readPlan, writePlan)
    }
  }

  private def callInTxConflict(
    readsAndWrites: ReadsAndWrites,
    wholePlan: LogicalPlan
  )(implicit planChildrenLookup: PlanChildrenLookup): Iterator[ConflictingPlanPair] = {
    if (readsAndWrites.reads.callInTxPlans.nonEmpty) {
      for {
        updatingPlan <-
          wholePlan.folder.findAllByClass[UpdatingPlan].iterator.filterNot(planChildrenLookup.isInTransactionalApply)
        txPlan <- readsAndWrites.reads.callInTxPlans.iterator
      } yield {
        ConflictingPlanPair(txPlan, Ref(updatingPlan), set1(EagernessReason.WriteAfterCallInTransactions))
      }
    } else Iterator.empty
  }

  // Conflicts between a Read and a Write
  sealed private trait ConflictType
  private case object ReadWriteConflict extends ConflictType
  private case object WriteReadConflict extends ConflictType

  /**
   * Add a DELETE conflict
   */
  private def deleteConflict(
    readVariable: LogicalVariable,
    readPlan: LogicalPlan,
    writePlan: LogicalPlan
  ): ConflictingPlanPair = {
    val conflict = Conflict(writePlan.id, readPlan.id)
    val reasons: Set[EagernessReason] = set1(ReadDeleteConflict(readVariable.name).withConflict(conflict))
    ConflictingPlanPair(Ref(writePlan), Ref(readPlan), reasons)
  }

  /**
   * Check if there is a DELETE overlap
   *
   * @param plansThatIntroduceVariable all plans that introduce the variable, with their predicates if they are leaf plans
   * @param predicatesOnDeletedEntity all predicates on the deleted node
   */
  private def deleteOverlaps(
    plansThatIntroduceVariable: Set[PlanThatIntroducesVariable],
    predicatesOnDeletedEntity: Seq[Expression]
  ): Boolean = {
    val readEntityPredicateCombinations: Set[Seq[Expression]] =
      if (plansThatIntroduceVariable.isEmpty) {
        // Variable was not introduced by a leaf plan
        Set(Seq.empty[Expression])
      } else {
        plansThatIntroduceVariable.map(_.predicates)
      }
    readEntityPredicateCombinations.exists(DeleteOverlaps.overlap(_, predicatesOnDeletedEntity) match {
      case DeleteOverlaps.NoLabelOverlap => false
      case _: DeleteOverlaps.Overlap     => true
    })
  }

  // This is used instead of a Set, since creating Sets is slow (if we do it a lot).
  private class ConflictingPlans(val p1: Ref[LogicalPlan], val p2: Ref[LogicalPlan]) {

    /*
     * Strongly inspired by [[MurmurHash3.unorderedHash]]
     */
    override def hashCode(): Int = {
      var a, b, n = 0
      var c = 1

      // p1
      {
        val x = p1
        val h: Int = x.##
        a += h
        b ^= h
        c *= h | 1
        n += 1
      }
      // p2
      {
        val x = p2
        val h: Int = x.##
        a += h
        b ^= h
        c *= h | 1
        n += 1
      }

      var h = MurmurHash3.setSeed
      h = MurmurHash3.mix(h, a)
      h = MurmurHash3.mix(h, b)
      h = MurmurHash3.mixLast(h, c)
      MurmurHash3.finalizeHash(h, n)
    }

    override def equals(obj: Any): Boolean = obj match {
      case cp: ConflictingPlans => (p1 == cp.p1 && p2 == cp.p2) || (p1 == cp.p2 && p2 == cp.p1)
      case _                    => false
    }
  }

  /**
   * By inspecting the reads and writes, return a [[ConflictingPlanPair]] for each Read/write conflict.
   * In the result there is only one ConflictingPlanPair per pair of plans, and the reasons are merged
   * if plans conflicts because of several reasons.
   */
  private[eager] def findConflictingPlans(
    readsAndWrites: ReadsAndWrites,
    wholePlan: LogicalPlan
  )(implicit planChildrenLookup: PlanChildrenLookup): Seq[ConflictingPlanPair] = {
    val leftMostLeaf = wholePlan.leftmostLeaf
    val map = mutable.Map[ConflictingPlans, mutable.Set[EagernessReason]]()

    def addConflict(conflictingPlanPair: ConflictingPlanPair): Unit = {
      map.getOrElseUpdate(
        new ConflictingPlans(conflictingPlanPair.first, conflictingPlanPair.second),
        mutable.Set.empty[EagernessReason]
      )
        .addAll(conflictingPlanPair.reasons)
    }

    // Conflict between a Node property read and a property write
    propertyConflicts(
      readsAndWrites,
      leftMostLeaf,
      _.writtenNodeProperties.nodeEntries,
      _.plansReadingNodeProperty(_)
    ).foreach(addConflict)

    // Conflict between a Relationship property read and a property write
    propertyConflicts(
      readsAndWrites,
      leftMostLeaf,
      _.writtenRelProperties.relEntries,
      _.plansReadingRelProperty(_)
    ).foreach(addConflict)

    // Conflicts between a label read and a label SET
    labelConflicts(readsAndWrites, leftMostLeaf).foreach(addConflict)

    // Conflicts between a label read (determined by a snapshot filterExpressions) and a label CREATE
    createNodeConflicts(
      readsAndWrites,
      leftMostLeaf
    ).foreach(addConflict)

    // Conflicts between a type read (determined by a snapshot filterExpressions) and a type CREATE
    createRelationshipConflicts(
      readsAndWrites,
      leftMostLeaf
    ).foreach(addConflict)

    // Conflicts between a MATCH and a DELETE with a node variable
    deleteVariableConflicts(
      readsAndWrites,
      leftMostLeaf,
      _.deletedNodeVariables,
      _.nodeFilterExpressions,
      _.possibleNodeDeleteConflictPlans,
      _.possibleNodeDeleteConflictPlanSnapshots(_)
    ).foreach(addConflict)

    // Conflicts between a MATCH and a DELETE with a relationship variable
    deleteVariableConflicts(
      readsAndWrites,
      leftMostLeaf,
      _.deletedRelationshipVariables,
      _.relationshipFilterExpressions,
      _.possibleRelDeleteConflictPlans,
      _.possibleRelationshipDeleteConflictPlanSnapshots(_)
    ).foreach(addConflict)

    // Conflicts between a MATCH and a DELETE with a node expression
    deleteExpressionConflicts(
      readsAndWrites,
      leftMostLeaf,
      _.plansThatDeleteNodeExpressions.iterator,
      _.possibleNodeDeleteConflictPlans,
      _.possibleNodeDeleteConflictPlanSnapshots(_)
    ).foreach(addConflict)

    // Conflicts between a MATCH and a DELETE with a relationship expression
    deleteExpressionConflicts(
      readsAndWrites,
      leftMostLeaf,
      _.plansThatDeleteRelationshipExpressions.iterator,
      _.possibleRelDeleteConflictPlans,
      _.possibleRelationshipDeleteConflictPlanSnapshots(_)
    ).foreach(addConflict)

    // Conflicts between a Updating Plan and a Call in Transaction.
    // This will find conflicts between updating clauses before a CALL IN TX as well, but that is already disallowed by semantic analysis.
    callInTxConflict(readsAndWrites, wholePlan).foreach(addConflict)

    map.map {
      case (conflictingPlans, reasons) => ConflictingPlanPair(conflictingPlans.p1, conflictingPlans.p2, reasons.toSet)
    }.toSeq
  }

  /**
   * Some conflicts can be disregarded if they access the same symbol through the same variable,
   * and if that variable is distinct when reading.
   */
  private def distinctConflictOnSameSymbol(
    read: PlanWithAccessor,
    write: PlanWithAccessor
  )(implicit planChildrenLookup: PlanChildrenLookup): Boolean = {
    read.accessor match {
      case Some(variable) =>
        write.accessor.contains(variable) && isGloballyUniqueAndCursorInitialized(
          variable,
          read.plan.value,
          write.plan.value
        )
      case None => false
    }
  }

  private def isValidConflict(
    readPlan: LogicalPlan,
    writePlan: LogicalPlan,
    leftMostLeaf: LogicalPlan
  )(implicit planChildrenLookup: PlanChildrenLookup): Boolean = {
    // A plan can never conflict with itself
    def conflictsWithItself = writePlan eq readPlan

    // a merge plan can never conflict with its children
    def mergeConflictWithChild = writePlan.isInstanceOf[Merge] && planChildrenLookup.hasChild(writePlan, readPlan)

    // a ForeachApply can conflict with its RHS children.
    // For now, we ignore those conflicts, to ensure side-effect visibility.
    def foreachConflictWithRHS: Boolean = {
      // Duplicate and less functional code for efficiency
      writePlan match {
        case ForeachApply(_, rhs, _, _) =>
          if ((rhs eq readPlan) || planChildrenLookup.hasChild(rhs, readPlan)) {
            return true
          }
        case _ =>
      }
      readPlan match {
        case ForeachApply(_, rhs, _, _) =>
          if ((rhs eq writePlan) || planChildrenLookup.hasChild(rhs, writePlan)) {
            return true
          }
        case _ =>
      }
      false
    }

    // We consider the leftmost plan to be potentially stable unless we are in a call in transactions.
    def conflictsWithUnstablePlan =
      (readPlan ne leftMostLeaf) ||
        !readPlan.isInstanceOf[StableLeafPlan] ||
        planChildrenLookup.isInTransactionalApply(writePlan)

    /**
     * Deleting plans can conflict, if they evaluate expressions. Otherwise not.
     */
    def simpleDeletingPlansConflict =
      simpleDeletingPlan(writePlan) && simpleDeletingPlan(readPlan)

    def nonConflictingReadPlan(): Boolean = readPlan.isInstanceOf[Argument]

    !nonConflictingReadPlan() &&
    !conflictsWithItself &&
    !simpleDeletingPlansConflict &&
    !mergeConflictWithChild &&
    !foreachConflictWithRHS &&
    conflictsWithUnstablePlan
  }

  private def isDistinctLeafPlan(readPlan: LogicalPlan): Boolean = readPlan match {
    case _: NodeLogicalLeafPlan           => true
    case rlp: RelationshipLogicalLeafPlan => rlp.directed
    case _                                => false
  }

  private def isSimpleDeleteAndDistinctLeaf(readPlan: LogicalPlan, writePlan: LogicalPlan): Boolean =
    simpleDeletingPlan(writePlan) && isDistinctLeafPlan(readPlan)

  private def isGloballyUniqueAndCursorInitialized(
    variable: LogicalVariable,
    readPlan: LogicalPlan,
    writePlan: LogicalPlan
  )(implicit planChildrenLookup: PlanChildrenLookup): Boolean = {
    // plan.distinctness is "per argument"
    // In order to make sure a column is "globally unique", i.e. over multiple invocations,
    // we need to make sure the operator does only execute once.
    // Also, we must make sure that the cursor performing the read will be initialized at the point in
    // time the write for the same variable happens.
    (isSimpleDeleteAndDistinctLeaf(readPlan, writePlan) || readPlan.distinctness.covers(
      Seq(variable)
    )) && !planChildrenLookup.readMightNotBeInitialized(readPlan)
  }

  /**
   * Tests whether a plan is a simple deleting plan that does not evaluate any expressions,
   * apart from the variable that it is supposed to delete. This variable read can never conflict
   * with another delete, even when the entity has already been deleted.
   * This is because deletes are idempotent.
   */
  private def simpleDeletingPlan(plan: LogicalPlan): Boolean = plan match {
    case DeleteNode(_, _: LogicalVariable)         => true
    case DetachDeleteNode(_, _: LogicalVariable)   => true
    case DeleteRelationship(_, _: LogicalVariable) => true
    case _                                         => false
  }
}

object ConflictFinder extends ConflictFinder {

  /**
   * Two plans that have a read/write conflict. The plans are in no particular order.
   *
   * @param first   one of the two plans.
   * @param second  the other plan.
   * @param reasons the reasons of the conflict.
   */
  private[eager] case class ConflictingPlanPair(
    first: Ref[LogicalPlan],
    second: Ref[LogicalPlan],
    reasons: Set[EagernessReason]
  )

  def withCaching(): ConflictFinder = {
    new ConflictFinderWithCaching()
  }

  private class ConflictFinderWithCaching() extends ConflictFinder {

    private val containsNestedPlanExpressionCache: Ref[LogicalPlan] => Boolean = {
      CachedFunction {
        lpRef => super.containsNestedPlanExpression(lpRef.value)
      }
    }

    override protected[eager] def containsNestedPlanExpression(lp: LogicalPlan): Boolean = {
      containsNestedPlanExpressionCache(Ref(lp))
    }
  }
}
