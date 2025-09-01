/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright Density Contributors. See
 * GitHub history for details.
 */

package org.density.painless.phase;

import org.density.painless.ir.BinaryImplNode;
import org.density.painless.ir.BinaryMathNode;
import org.density.painless.ir.BlockNode;
import org.density.painless.ir.BooleanNode;
import org.density.painless.ir.BreakNode;
import org.density.painless.ir.CastNode;
import org.density.painless.ir.CatchNode;
import org.density.painless.ir.ClassNode;
import org.density.painless.ir.ComparisonNode;
import org.density.painless.ir.ConditionalNode;
import org.density.painless.ir.ConstantNode;
import org.density.painless.ir.ContinueNode;
import org.density.painless.ir.DeclarationBlockNode;
import org.density.painless.ir.DeclarationNode;
import org.density.painless.ir.DefInterfaceReferenceNode;
import org.density.painless.ir.DoWhileLoopNode;
import org.density.painless.ir.DupNode;
import org.density.painless.ir.ElvisNode;
import org.density.painless.ir.FieldNode;
import org.density.painless.ir.FlipArrayIndexNode;
import org.density.painless.ir.FlipCollectionIndexNode;
import org.density.painless.ir.FlipDefIndexNode;
import org.density.painless.ir.ForEachLoopNode;
import org.density.painless.ir.ForEachSubArrayNode;
import org.density.painless.ir.ForEachSubIterableNode;
import org.density.painless.ir.ForLoopNode;
import org.density.painless.ir.FunctionNode;
import org.density.painless.ir.IfElseNode;
import org.density.painless.ir.IfNode;
import org.density.painless.ir.InstanceofNode;
import org.density.painless.ir.InvokeCallDefNode;
import org.density.painless.ir.InvokeCallMemberNode;
import org.density.painless.ir.InvokeCallNode;
import org.density.painless.ir.ListInitializationNode;
import org.density.painless.ir.LoadBraceDefNode;
import org.density.painless.ir.LoadBraceNode;
import org.density.painless.ir.LoadDotArrayLengthNode;
import org.density.painless.ir.LoadDotDefNode;
import org.density.painless.ir.LoadDotNode;
import org.density.painless.ir.LoadDotShortcutNode;
import org.density.painless.ir.LoadFieldMemberNode;
import org.density.painless.ir.LoadListShortcutNode;
import org.density.painless.ir.LoadMapShortcutNode;
import org.density.painless.ir.LoadVariableNode;
import org.density.painless.ir.MapInitializationNode;
import org.density.painless.ir.NewArrayNode;
import org.density.painless.ir.NewObjectNode;
import org.density.painless.ir.NullNode;
import org.density.painless.ir.NullSafeSubNode;
import org.density.painless.ir.ReturnNode;
import org.density.painless.ir.StatementExpressionNode;
import org.density.painless.ir.StaticNode;
import org.density.painless.ir.StoreBraceDefNode;
import org.density.painless.ir.StoreBraceNode;
import org.density.painless.ir.StoreDotDefNode;
import org.density.painless.ir.StoreDotNode;
import org.density.painless.ir.StoreDotShortcutNode;
import org.density.painless.ir.StoreFieldMemberNode;
import org.density.painless.ir.StoreListShortcutNode;
import org.density.painless.ir.StoreMapShortcutNode;
import org.density.painless.ir.StoreVariableNode;
import org.density.painless.ir.StringConcatenationNode;
import org.density.painless.ir.ThrowNode;
import org.density.painless.ir.TryNode;
import org.density.painless.ir.TypedCaptureReferenceNode;
import org.density.painless.ir.TypedInterfaceReferenceNode;
import org.density.painless.ir.UnaryMathNode;
import org.density.painless.ir.WhileLoopNode;

public interface IRTreeVisitor<Scope> {

    void visitClass(ClassNode irClassNode, Scope scope);

    void visitFunction(FunctionNode irFunctionNode, Scope scope);

    void visitField(FieldNode irFieldNode, Scope scope);

    void visitBlock(BlockNode irBlockNode, Scope scope);

    void visitIf(IfNode irIfNode, Scope scope);

    void visitIfElse(IfElseNode irIfElseNode, Scope scope);

    void visitWhileLoop(WhileLoopNode irWhileLoopNode, Scope scope);

    void visitDoWhileLoop(DoWhileLoopNode irDoWhileLoopNode, Scope scope);

    void visitForLoop(ForLoopNode irForLoopNode, Scope scope);

    void visitForEachLoop(ForEachLoopNode irForEachLoopNode, Scope scope);

    void visitForEachSubArrayLoop(ForEachSubArrayNode irForEachSubArrayNode, Scope scope);

    void visitForEachSubIterableLoop(ForEachSubIterableNode irForEachSubIterableNode, Scope scope);

    void visitDeclarationBlock(DeclarationBlockNode irDeclarationBlockNode, Scope scope);

    void visitDeclaration(DeclarationNode irDeclarationNode, Scope scope);

    void visitReturn(ReturnNode irReturnNode, Scope scope);

    void visitStatementExpression(StatementExpressionNode irStatementExpressionNode, Scope scope);

    void visitTry(TryNode irTryNode, Scope scope);

    void visitCatch(CatchNode irCatchNode, Scope scope);

    void visitThrow(ThrowNode irThrowNode, Scope scope);

    void visitContinue(ContinueNode irContinueNode, Scope scope);

    void visitBreak(BreakNode irBreakNode, Scope scope);

    void visitBinaryImpl(BinaryImplNode irBinaryImplNode, Scope scope);

    void visitUnaryMath(UnaryMathNode irUnaryMathNode, Scope scope);

    void visitBinaryMath(BinaryMathNode irBinaryMathNode, Scope scope);

    void visitStringConcatenation(StringConcatenationNode irStringConcatenationNode, Scope scope);

    void visitBoolean(BooleanNode irBooleanNode, Scope scope);

    void visitComparison(ComparisonNode irComparisonNode, Scope scope);

    void visitCast(CastNode irCastNode, Scope scope);

    void visitInstanceof(InstanceofNode irInstanceofNode, Scope scope);

    void visitConditional(ConditionalNode irConditionalNode, Scope scope);

    void visitElvis(ElvisNode irElvisNode, Scope scope);

    void visitListInitialization(ListInitializationNode irListInitializationNode, Scope scope);

    void visitMapInitialization(MapInitializationNode irMapInitializationNode, Scope scope);

    void visitNewArray(NewArrayNode irNewArrayNode, Scope scope);

    void visitNewObject(NewObjectNode irNewObjectNode, Scope scope);

    void visitConstant(ConstantNode irConstantNode, Scope scope);

    void visitNull(NullNode irNullNode, Scope scope);

    void visitDefInterfaceReference(DefInterfaceReferenceNode irDefInterfaceReferenceNode, Scope scope);

    void visitTypedInterfaceReference(TypedInterfaceReferenceNode irTypedInterfaceReferenceNode, Scope scope);

    void visitTypeCaptureReference(TypedCaptureReferenceNode irTypedCaptureReferenceNode, Scope scope);

    void visitStatic(StaticNode irStaticNode, Scope scope);

    void visitLoadVariable(LoadVariableNode irLoadVariableNode, Scope scope);

    void visitNullSafeSub(NullSafeSubNode irNullSafeSubNode, Scope scope);

    void visitLoadDotArrayLengthNode(LoadDotArrayLengthNode irLoadDotArrayLengthNode, Scope scope);

    void visitLoadDotDef(LoadDotDefNode irLoadDotDefNode, Scope scope);

    void visitLoadDot(LoadDotNode irLoadDotNode, Scope scope);

    void visitLoadDotShortcut(LoadDotShortcutNode irDotSubShortcutNode, Scope scope);

    void visitLoadListShortcut(LoadListShortcutNode irLoadListShortcutNode, Scope scope);

    void visitLoadMapShortcut(LoadMapShortcutNode irLoadMapShortcutNode, Scope scope);

    void visitLoadFieldMember(LoadFieldMemberNode irLoadFieldMemberNode, Scope scope);

    void visitLoadBraceDef(LoadBraceDefNode irLoadBraceDefNode, Scope scope);

    void visitLoadBrace(LoadBraceNode irLoadBraceNode, Scope scope);

    void visitStoreVariable(StoreVariableNode irStoreVariableNode, Scope scope);

    void visitStoreDotDef(StoreDotDefNode irStoreDotDefNode, Scope scope);

    void visitStoreDot(StoreDotNode irStoreDotNode, Scope scope);

    void visitStoreDotShortcut(StoreDotShortcutNode irDotSubShortcutNode, Scope scope);

    void visitStoreListShortcut(StoreListShortcutNode irStoreListShortcutNode, Scope scope);

    void visitStoreMapShortcut(StoreMapShortcutNode irStoreMapShortcutNode, Scope scope);

    void visitStoreFieldMember(StoreFieldMemberNode irStoreFieldMemberNode, Scope scope);

    void visitStoreBraceDef(StoreBraceDefNode irStoreBraceDefNode, Scope scope);

    void visitStoreBrace(StoreBraceNode irStoreBraceNode, Scope scope);

    void visitInvokeCallDef(InvokeCallDefNode irInvokeCallDefNode, Scope scope);

    void visitInvokeCall(InvokeCallNode irInvokeCallNode, Scope scope);

    void visitInvokeCallMember(InvokeCallMemberNode irInvokeCallMemberNode, Scope scope);

    void visitFlipArrayIndex(FlipArrayIndexNode irFlipArrayIndexNode, Scope scope);

    void visitFlipCollectionIndex(FlipCollectionIndexNode irFlipCollectionIndexNode, Scope scope);

    void visitFlipDefIndex(FlipDefIndexNode irFlipDefIndexNode, Scope scope);

    void visitDup(DupNode irDupNode, Scope scope);
}
