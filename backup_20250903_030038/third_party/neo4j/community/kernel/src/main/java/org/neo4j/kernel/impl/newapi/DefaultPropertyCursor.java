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
package org.neo4j.kernel.impl.newapi;

import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_ENTITY;
import static org.neo4j.storageengine.api.LongReference.NULL_REFERENCE;
import static org.neo4j.storageengine.api.PropertySelection.ALL_PROPERTIES;
import static org.neo4j.token.api.TokenConstants.NO_TOKEN;

import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelTypeSupplier;
import org.neo4j.internal.kernel.api.TokenSet;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.ReadSecurityPropertyProvider;
import org.neo4j.kernel.api.AccessModeProvider;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.storageengine.api.LongReference;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.Reference;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.txstate.EntityState;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

public class DefaultPropertyCursor extends TraceableCursorImpl<DefaultPropertyCursor>
        implements PropertyCursor, Supplier<TokenSet>, RelTypeSupplier {
    private static final int NODE = -2;
    final StoragePropertyCursor storeCursor;
    private final InternalCursorFactory internalCursors;
    private final boolean applyAccessModeToTxState;
    private Read read;
    private AccessModeProvider accessModeProvider;
    private StoragePropertyCursor securityPropertyCursor;
    private FullAccessNodeCursor securityNodeCursor;
    private FullAccessRelationshipScanCursor securityRelCursor;
    private EntityState propertiesState;
    private Iterator<StorageProperty> txStateChangedProperties;
    private StorageProperty txStateValue;
    private long entityReference = LongReference.NULL;
    private TokenSet labels;
    // stores relationship type or NODE if not a relationship
    private int type = NO_TOKEN;
    private boolean addedInTx;
    private PropertySelection selection;
    private ReadSecurityPropertyProvider securityPropertyProvider;

    DefaultPropertyCursor(
            CursorPool<DefaultPropertyCursor> pool,
            StoragePropertyCursor storeCursor,
            InternalCursorFactory internalCursors,
            boolean applyAccessModeToTxState) {
        super(pool);
        this.storeCursor = storeCursor;
        this.internalCursors = internalCursors;
        this.applyAccessModeToTxState = applyAccessModeToTxState;
    }

    void initNode(
            long nodeReference,
            Reference reference,
            PropertySelection selection,
            Read read,
            TxStateHolder txStateHolder,
            AccessModeProvider accessModeProvider) {
        assert nodeReference != LongReference.NULL;

        init(selection, read, accessModeProvider);
        this.type = NODE;
        initializeNodeTransactionState(nodeReference, txStateHolder);
        storeCursor.initNodeProperties(reference, filterSelectionForTxState(selection));
        initSecurityPropertyProvision(
                (propertyCursor, propertySelection) -> propertyCursor.initNodeProperties(reference, propertySelection));
        this.entityReference = nodeReference;
    }

    void initNode(
            DefaultNodeCursor nodeCursor,
            PropertySelection selection,
            Read read,
            boolean initStoreCursor,
            TxStateHolder txStateHolder,
            AccessModeProvider accessModeProvider) {
        entityReference = nodeCursor.nodeReference();
        assert entityReference != LongReference.NULL;

        init(selection, read, accessModeProvider);
        this.type = NODE;
        this.addedInTx = nodeCursor.currentNodeIsAddedInTx();
        initializeNodeTransactionState(entityReference, txStateHolder);
        if (!addedInTx || applyAccessModeToTxState) {
            if (initStoreCursor) {
                storeCursor.initNodeProperties(nodeCursor.storeCursor, filterSelectionForTxState(selection));
            } // else it has already been externally initialized
            initSecurityPropertyProvision((propertyCursor, propertySelection) ->
                    propertyCursor.initNodeProperties(nodeCursor.storeCursor, propertySelection));
        } else {
            storeCursor.reset();
            securityPropertyProvider = null;
        }
    }

    /**
     * Given the {@link PropertySelection} from the initial request, this narrows it down even further,
     * removing keys that has been changed or removed for this entity so that they don't have to be
     * selected from storage cursor. The returned {@link PropertySelection} should be passed to storage cursor.
     */
    private PropertySelection filterSelectionForTxState(PropertySelection selection) {
        // We're giving the entity state to the created selection here, which could be non-ideal if
        // this created selection is kept around in a larger context. But here it isn't.
        return propertiesState == null || propertiesState == EntityState.EMPTY
                ? selection
                : selection.excluding(k -> propertiesState.isPropertyChangedOrRemoved(k));
    }

    void initSecurityPropertyProvision(BiConsumer<StoragePropertyCursor, PropertySelection> initNodeProperties) {
        AccessMode accessMode = accessModeProvider.getAccessMode();
        securityPropertyProvider = null;
        if (internalCursors == null || !accessMode.hasPropertyReadRules()) {
            return;
        }
        // We have property read rules
        PropertySelection securityProperties = accessMode.getSecurityPropertySelection(selection);
        if (securityProperties == null) {
            // The property read rules were not relevant to this `selection`.
            return;
        }
        // We have RELEVANT property read rules (i.e. they pertain to the `selection`)
        initNodeProperties.accept(lazyInitAndGetSecurityPropertyCursor(), securityProperties);
        securityPropertyProvider = new ReadSecurityPropertyProvider.LazyReadSecurityPropertyProvider(
                securityPropertyCursor,
                applyAccessModeToTxState && this.propertiesState != null
                        ? this.propertiesState.addedAndChangedProperties()
                        : null,
                securityProperties);
    }

    private StoragePropertyCursor lazyInitAndGetSecurityPropertyCursor() {
        if (securityPropertyCursor == null) {
            securityPropertyCursor = internalCursors.allocateStoragePropertyCursor();
        }
        return securityPropertyCursor;
    }

    private void initializeNodeTransactionState(long nodeReference, TxStateHolder txStateHolder) {
        if (txStateHolder.hasTxStateWithChanges()) {
            this.propertiesState = txStateHolder.txState().getNodeState(nodeReference);
            this.txStateChangedProperties =
                    this.propertiesState.addedAndChangedProperties().iterator();
        } else {
            this.propertiesState = null;
            this.txStateChangedProperties = null;
        }
    }

    void initRelationship(
            long relationshipReference,
            Reference reference,
            PropertySelection selection,
            Read read,
            TxStateHolder txStateHolder,
            AccessModeProvider accessModeProvider) {
        assert relationshipReference != LongReference.NULL;

        init(selection, read, accessModeProvider);
        initializeRelationshipTransactionState(relationshipReference, txStateHolder);
        storeCursor.initRelationshipProperties(reference, filterSelectionForTxState(selection));
        this.entityReference = relationshipReference;
    }

    void initRelationship(
            DefaultRelationshipCursor relationshipCursor,
            PropertySelection selection,
            Read read,
            TxStateHolder txStateHolder,
            AccessModeProvider accessModeProvider) {
        entityReference = relationshipCursor.relationshipReference();
        assert entityReference != LongReference.NULL;

        init(selection, read, accessModeProvider);
        initializeRelationshipTransactionState(entityReference, txStateHolder);
        this.addedInTx = relationshipCursor.currentRelationshipIsAddedInTx();
        if (!addedInTx || applyAccessModeToTxState) {
            storeCursor.initRelationshipProperties(
                    relationshipCursor.storeCursor, filterSelectionForTxState(selection));
        } else {
            storeCursor.reset();
        }
    }

    private void initializeRelationshipTransactionState(long relationshipReference, TxStateHolder txStateHolder) {
        // Transaction state
        if (txStateHolder.hasTxStateWithChanges()) {
            this.propertiesState = txStateHolder.txState().getRelationshipState(relationshipReference);
            this.txStateChangedProperties =
                    this.propertiesState.addedAndChangedProperties().iterator();
        } else {
            this.propertiesState = null;
            this.txStateChangedProperties = null;
        }
    }

    void initEmptyRelationship() {
        init(ALL_PROPERTIES, null, null);
        storeCursor.initRelationshipProperties(NULL_REFERENCE, ALL_PROPERTIES);
        this.entityReference = NO_SUCH_ENTITY;

        this.propertiesState = null;
        this.txStateChangedProperties = null;
    }

    private void init(PropertySelection selection, Read read, AccessModeProvider accessModeProvider) {
        this.selection = selection;
        this.read = read;
        this.accessModeProvider = accessModeProvider;
        this.labels = null;
        this.type = NO_TOKEN;
    }

    boolean allowed(int[] propertyKeys, int[] labels) {
        AccessMode accessMode = accessModeProvider.getAccessMode();
        if (isNode()) {
            return accessMode.allowsReadNodeProperties(
                    () -> Labels.from(labels), propertyKeys, securityPropertyProvider);
        }

        for (int propertyKey : propertyKeys) {
            if (!accessMode.allowsReadRelationshipProperty(this, propertyKey)) {
                return false;
            }
        }
        return true;
    }

    protected boolean allowed(int propertyKey) {
        AccessMode accessMode = accessModeProvider.getAccessMode();
        if (isNode()) {
            return accessMode.allowsReadNodeProperty(this, propertyKey, securityPropertyProvider);
        } else {
            return accessMode.allowsReadRelationshipProperty(this, propertyKey);
        }
    }

    @Override
    public boolean next() {
        if (txStateChangedProperties != null) {
            while (txStateChangedProperties.hasNext()) {
                txStateValue = txStateChangedProperties.next();
                int propertyKey = txStateValue.propertyKeyId();
                if (selection.test(propertyKey)) {
                    if (!applyAccessModeToTxState || allowed(propertyKey)) {
                        if (tracer != null) {
                            tracer.onProperty(propertyKey);
                        }
                        return true;
                    }
                }
            }
            txStateChangedProperties = null;
            txStateValue = null;
        }

        while (storeCursor.next()) {
            int propertyKey = storeCursor.propertyKey();
            if (allowed(propertyKey)) {
                if (tracer != null) {
                    tracer.onProperty(propertyKey);
                }
                return true;
            }
        }
        return false;
    }

    @Override
    public void closeInternal() {
        if (!isClosed()) {
            propertiesState = null;
            txStateChangedProperties = null;
            txStateValue = null;
            read = null;
            accessModeProvider = null;
            storeCursor.reset();
            if (securityPropertyCursor != null) {
                securityPropertyCursor.reset();
            }
            securityPropertyProvider = null;
        }
        super.closeInternal();
    }

    @Override
    public int propertyKey() {
        if (txStateValue != null) {
            return txStateValue.propertyKeyId();
        }
        return storeCursor.propertyKey();
    }

    @Override
    public ValueGroup propertyType() {
        if (txStateValue != null) {
            return txStateValue.value().valueGroup();
        }
        return storeCursor.propertyType();
    }

    @Override
    public Value propertyValue() {
        if (txStateValue != null) {
            return txStateValue.value();
        }

        return storeCursor.propertyValue();
    }

    @Override
    public boolean isClosed() {
        return read == null;
    }

    @Override
    public String toString() {
        if (isClosed()) {
            return "PropertyCursor[closed state]";
        } else {
            return "PropertyCursor[id=" + propertyKey() + ", " + storeCursor + " ]";
        }
    }

    /**
     * Gets the label while ignoring removes in the tx state. Implemented as a Supplier so that we don't need additional
     * allocations.
     *
     * Only used for security checks
     */
    @Override
    public TokenSet get() {
        assert isNode();

        if (labels == null) {
            if (securityNodeCursor == null) {
                securityNodeCursor = internalCursors.allocateFullAccessNodeCursor();
            }
            read.singleNode(entityReference, securityNodeCursor);
            securityNodeCursor.next();
            if (applyAccessModeToTxState) {
                labels = securityNodeCursor.labels();
            } else {
                labels = securityNodeCursor.labelsIgnoringTxStateSetRemove();
            }
        }
        return labels;
    }

    /**
     * Only used for security checks
     */
    @Override
    public int getRelType() {
        assert isRelationship();

        if (type < 0) {
            if (securityRelCursor == null) {
                securityRelCursor = internalCursors.allocateFullAccessRelationshipScanCursor();
            }
            read.singleRelationship(entityReference, securityRelCursor);
            securityRelCursor.next();
            this.type = securityRelCursor.type();
        }
        return type;
    }

    @Override
    public void release() {
        if (storeCursor != null) {
            storeCursor.close();
        }
        if (securityPropertyCursor != null) {
            securityPropertyCursor.close();
            securityPropertyCursor = null;
        }
        if (securityNodeCursor != null) {
            securityNodeCursor.close();
            securityNodeCursor.release();
            securityNodeCursor = null;
        }
        if (securityRelCursor != null) {
            securityRelCursor.close();
            securityRelCursor.release();
            securityRelCursor = null;
        }
    }

    private boolean isNode() {
        return type == NODE;
    }

    private boolean isRelationship() {
        return type != NODE;
    }
}
