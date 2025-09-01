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

import org.density.painless.node.EAssignment;
import org.density.painless.node.EBinary;
import org.density.painless.node.EBooleanComp;
import org.density.painless.node.EBooleanConstant;
import org.density.painless.node.EBrace;
import org.density.painless.node.ECall;
import org.density.painless.node.ECallLocal;
import org.density.painless.node.EComp;
import org.density.painless.node.EConditional;
import org.density.painless.node.EDecimal;
import org.density.painless.node.EDot;
import org.density.painless.node.EElvis;
import org.density.painless.node.EExplicit;
import org.density.painless.node.EFunctionRef;
import org.density.painless.node.EInstanceof;
import org.density.painless.node.ELambda;
import org.density.painless.node.EListInit;
import org.density.painless.node.EMapInit;
import org.density.painless.node.ENewArray;
import org.density.painless.node.ENewArrayFunctionRef;
import org.density.painless.node.ENewObj;
import org.density.painless.node.ENull;
import org.density.painless.node.ENumeric;
import org.density.painless.node.ERegex;
import org.density.painless.node.EString;
import org.density.painless.node.ESymbol;
import org.density.painless.node.EUnary;
import org.density.painless.node.SBlock;
import org.density.painless.node.SBreak;
import org.density.painless.node.SCatch;
import org.density.painless.node.SClass;
import org.density.painless.node.SContinue;
import org.density.painless.node.SDeclBlock;
import org.density.painless.node.SDeclaration;
import org.density.painless.node.SDo;
import org.density.painless.node.SEach;
import org.density.painless.node.SExpression;
import org.density.painless.node.SFor;
import org.density.painless.node.SFunction;
import org.density.painless.node.SIf;
import org.density.painless.node.SIfElse;
import org.density.painless.node.SReturn;
import org.density.painless.node.SThrow;
import org.density.painless.node.STry;
import org.density.painless.node.SWhile;

public interface UserTreeVisitor<Scope> {

    void visitClass(SClass userClassNode, Scope scope);

    void visitFunction(SFunction userFunctionNode, Scope scope);

    void visitBlock(SBlock userBlockNode, Scope scope);

    void visitIf(SIf userIfNode, Scope scope);

    void visitIfElse(SIfElse userIfElseNode, Scope scope);

    void visitWhile(SWhile userWhileNode, Scope scope);

    void visitDo(SDo userDoNode, Scope scope);

    void visitFor(SFor userForNode, Scope scope);

    void visitEach(SEach userEachNode, Scope scope);

    void visitDeclBlock(SDeclBlock userDeclBlockNode, Scope scope);

    void visitDeclaration(SDeclaration userDeclarationNode, Scope scope);

    void visitReturn(SReturn userReturnNode, Scope scope);

    void visitExpression(SExpression userExpressionNode, Scope scope);

    void visitTry(STry userTryNode, Scope scope);

    void visitCatch(SCatch userCatchNode, Scope scope);

    void visitThrow(SThrow userThrowNode, Scope scope);

    void visitContinue(SContinue userContinueNode, Scope scope);

    void visitBreak(SBreak userBreakNode, Scope scope);

    void visitAssignment(EAssignment userAssignmentNode, Scope scope);

    void visitUnary(EUnary userUnaryNode, Scope scope);

    void visitBinary(EBinary userBinaryNode, Scope scope);

    void visitBooleanComp(EBooleanComp userBooleanCompNode, Scope scope);

    void visitComp(EComp userCompNode, Scope scope);

    void visitExplicit(EExplicit userExplicitNode, Scope scope);

    void visitInstanceof(EInstanceof userInstanceofNode, Scope scope);

    void visitConditional(EConditional userConditionalNode, Scope scope);

    void visitElvis(EElvis userElvisNode, Scope scope);

    void visitListInit(EListInit userListInitNode, Scope scope);

    void visitMapInit(EMapInit userMapInitNode, Scope scope);

    void visitNewArray(ENewArray userNewArrayNode, Scope scope);

    void visitNewObj(ENewObj userNewObjectNode, Scope scope);

    void visitCallLocal(ECallLocal userCallLocalNode, Scope scope);

    void visitBooleanConstant(EBooleanConstant userBooleanConstantNode, Scope scope);

    void visitNumeric(ENumeric userNumericNode, Scope scope);

    void visitDecimal(EDecimal userDecimalNode, Scope scope);

    void visitString(EString userStringNode, Scope scope);

    void visitNull(ENull userNullNode, Scope scope);

    void visitRegex(ERegex userRegexNode, Scope scope);

    void visitLambda(ELambda userLambdaNode, Scope scope);

    void visitFunctionRef(EFunctionRef userFunctionRefNode, Scope scope);

    void visitNewArrayFunctionRef(ENewArrayFunctionRef userNewArrayFunctionRefNode, Scope scope);

    void visitSymbol(ESymbol userSymbolNode, Scope scope);

    void visitDot(EDot userDotNode, Scope scope);

    void visitBrace(EBrace userBraceNode, Scope scope);

    void visitCall(ECall userCallNode, Scope scope);
}
