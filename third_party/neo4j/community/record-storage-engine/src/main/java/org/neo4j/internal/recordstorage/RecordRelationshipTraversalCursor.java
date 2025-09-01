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
package org.neo4j.internal.recordstorage;

import static org.neo4j.storageengine.api.RelationshipDirection.INCOMING;
import static org.neo4j.storageengine.api.RelationshipDirection.LOOP;
import static org.neo4j.storageengine.api.RelationshipDirection.OUTGOING;
import static org.neo4j.storageengine.api.RelationshipDirection.directionOfStrict;

import org.neo4j.internal.counts.RelationshipGroupDegreesStore;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.LongReference;
import org.neo4j.storageengine.api.ReadTracer;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.string.Mask;

class RecordRelationshipTraversalCursor extends RecordRelationshipCursor implements StorageRelationshipTraversalCursor {
    private final StoreCursors storeCursors;
    private ReadTracer tracer;

    private enum GroupState {
        INCOMING,
        OUTGOING,
        LOOP,
        NONE
    }

    private RelationshipSelection selection;
    private long originNodeReference;
    private long next = LongReference.NULL;
    private PageCursor pageCursor;
    private final RecordRelationshipGroupCursor group;
    private GroupState groupState = GroupState.NONE;
    private boolean open;

    RecordRelationshipTraversalCursor(
            RelationshipStore relationshipStore,
            RelationshipGroupStore groupStore,
            RelationshipGroupDegreesStore groupDegreesStore,
            CursorContext cursorContext,
            StoreCursors storeCursors,
            MemoryTracker memoryTracker) {
        super(relationshipStore, cursorContext, memoryTracker);
        this.storeCursors = storeCursors;
        this.group = new RecordRelationshipGroupCursor(
                relationshipStore, groupStore, groupDegreesStore, loadMode, cursorContext, storeCursors, memoryTracker);
    }

    void init(RecordNodeCursor nodeCursor, RelationshipSelection selection) {
        init(nodeCursor.entityReference(), nodeCursor.getNextRel(), nodeCursor.isDense(), selection);
    }

    @Override
    public void init(long nodeReference, long reference, RelationshipSelection selection) {
        if (reference == LongReference.NULL) {
            resetState();
            return;
        }

        RelationshipReferenceEncoding encoding = RelationshipReferenceEncoding.parseEncoding(reference);
        reference = RelationshipReferenceEncoding.clearEncoding(reference);

        init(nodeReference, reference, encoding == RelationshipReferenceEncoding.DENSE, selection);
    }

    private void init(long nodeReference, long reference, boolean isDense, RelationshipSelection selection) {
        if (reference == LongReference.NULL) {
            resetState();
            return;
        }

        this.selection = selection;
        if (isDense) {
            // The reference points to a relationship group record
            groups(nodeReference, reference);
        } else {
            // The reference points to a relationship record
            chain(nodeReference, reference);
        }
        open = true;
    }

    /*
     * Normal traversal. Traversal returns mixed types and directions.
     */
    private void chain(long nodeReference, long reference) {
        ensureCursor();
        setId(LongReference.NULL);
        this.groupState = GroupState.NONE;
        this.originNodeReference = nodeReference;
        this.next = reference;
    }

    /*
     * Reference to a group record. Traversal returns mixed types and directions.
     */
    private void groups(long nodeReference, long groupReference) {
        setId(LongReference.NULL);
        this.next = LongReference.NULL;
        this.groupState = GroupState.INCOMING;
        this.originNodeReference = nodeReference;
        this.group.direct(nodeReference, groupReference);
    }

    @Override
    public long neighbourNodeReference() {
        final long source = sourceNodeReference(), target = targetNodeReference();
        if (source == originNodeReference) {
            return target;
        } else if (target == originNodeReference) {
            return source;
        } else {
            throw new IllegalStateException("NOT PART OF CHAIN");
        }
    }

    @Override
    public long originNodeReference() {
        return originNodeReference;
    }

    @Override
    public boolean next() {
        boolean traversingDenseNode;
        do {
            traversingDenseNode = traversingDenseNode();
            if (traversingDenseNode) {
                traverseDenseNode();
            }

            if (next == LongReference.NULL) {
                resetState();
                return false;
            }

            relationshipFull(this, next, pageCursor);
            computeNext();
            if (tracer != null) {
                tracer.onRelationship(entityReference());
            }
        } while (!inUse()
                || (!traversingDenseNode
                        && !selection.test(
                                getType(), directionOfStrict(originNodeReference, getFirstNode(), getSecondNode()))));
        return true;
    }

    private void traverseDenseNode() {
        while (next == LongReference.NULL) {
            /*
             Dense nodes looks something like:

                   Node(dense=true)

                           |
                           v

                       Group(:HOLDS)   -incoming-> Rel(id=2) -> Rel(id=3)
                                       -outgoing-> Rel(id=5) -> Rel(id=10) -> Rel(id=3)
                                       -loop->     Rel(id=9)
                           |
                           v

                       Group(:USES)    -incoming-> Rel(id=14)
                                       -outgoing-> Rel(id=55) -> Rel(id=51) -> ...
                                       -loop->     Rel(id=21) -> Rel(id=11)

                           |
                           v
                           ...

             We iterate over dense nodes using a small state machine staring in state INCOMING.
             1) fetch next group, if no more group stop.
             2) set next to group.incomingReference, switch state to OUTGOING
             3) Iterate relationship chain until we reach the end
             4) set next to group.outgoingReference and state to LOOP
             5) Iterate relationship chain until we reach the end
             6) set next to group.loop and state back to INCOMING
             7) Iterate relationship chain until we reach the end
             8) GOTO 1
            */
            switch (groupState) {
                case INCOMING:
                    boolean hasNext = group.next();
                    if (!hasNext) {
                        assert next == LongReference.NULL;
                        return; // no more groups nor relationships
                    }
                    if (tracer != null) {
                        tracer.dbHit();
                    }
                    if (group.getType() > selection.highestType()) {
                        // The groups are ordered, no need to keep looking if we are past the end of selection
                        return;
                    }
                    if (!selection.test(group.getType())) {
                        // This type isn't part of this selection, so skip the whole group
                        continue;
                    }

                    if (selection.test(group.getType(), INCOMING)) {
                        next = group.incomingRawId();
                        ensureCursor();
                    }
                    groupState = GroupState.OUTGOING;
                    break;

                case OUTGOING:
                    if (selection.test(group.getType(), OUTGOING)) {
                        ensureCursor();
                        next = group.outgoingRawId();
                    }
                    groupState = GroupState.LOOP;
                    break;

                case LOOP:
                    if (selection.test(group.getType(), LOOP)) {
                        ensureCursor();
                        next = group.loopsRawId();
                    }
                    groupState = GroupState.INCOMING;
                    break;

                default:
                    throw new IllegalStateException("We cannot get here, but checkstyle forces this!");
            }
        }
    }

    private void ensureCursor() {
        if (pageCursor == null) {
            pageCursor = storeCursors.readCursor(RecordCursorTypes.RELATIONSHIP_CURSOR);
        }
    }

    private void computeNext() {
        final long source = sourceNodeReference(), target = targetNodeReference();
        if (source == originNodeReference) {
            next = getFirstNextRel();
        } else if (target == originNodeReference) {
            next = getSecondNextRel();
        } else {
            throw new IllegalStateException("NOT PART OF CHAIN! " + this);
        }
    }

    private boolean traversingDenseNode() {
        return groupState != GroupState.NONE;
    }

    @Override
    public void reset() {
        if (open) {
            open = false;
            resetState();
        }
    }

    @Override
    public void setTracer(ReadTracer tracer) {
        // Since this cursor does its own filtering on relationships and has internal relationship group records and
        // such,
        // the kernel can't possible tell the number of db hits and therefore we do it here in this cursor instead.
        this.tracer = tracer;
    }

    @Override
    public void removeTracer() {
        this.tracer = null;
    }

    @Override
    public void setForceLoad() {
        super.setForceLoad();
        group.loadMode = loadMode;
    }

    @Override
    protected void resetState() {
        super.resetState();
        group.loadMode = loadMode;
        setId(next = LongReference.NULL);
        groupState = GroupState.NONE;
        selection = null;
    }

    @Override
    public void close() {
        group.close();
        pageCursor = null; // Cursor owned by StoreCursors cache so not closed here
    }

    @Override
    public String toString(Mask mask) {
        if (!open) {
            return "RelationshipTraversalCursor[closed state]";
        } else {
            String dense = "denseNode=" + traversingDenseNode();
            return "RelationshipTraversalCursor[id=" + getId() + ", open state with: "
                    + dense + ", next="
                    + next + ", underlying record="
                    + super.toString(mask) + "]";
        }
    }
}
