/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.expressions;

import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.ValidationException;

public class ExpressionRemove {
  private final Set<Integer> ids;
  private final Schema schema;

  public ExpressionRemove(Schema schema) {
    this.ids = schema.identifierFieldIds();
    this.schema = schema;
  }

  public Expression visitEvaluator(Expression expr, boolean caseSensitive) {
    if (expr instanceof Predicate) {
      if (expr instanceof BoundPredicate) {
        BoundPredicate<?> boundPre = (BoundPredicate<?>) expr;
        return getBoundExpression(expr, boundPre);
      } else {
        UnboundPredicate<?> unboundPre = (UnboundPredicate<?>) expr;
        BoundPredicate<?> boundPre = (BoundPredicate<?>) (unboundPre.bind(schema.asStruct(), caseSensitive));
        return getBoundExpression(expr, boundPre);
      }
    } else {
      switch (expr.op()) {
        case TRUE:
          return True.INSTANCE;
        case FALSE:
          return False.INSTANCE;
        case NOT:
          Not not = (Not) expr;
          Expression notExpression = visitEvaluator(not.child(), caseSensitive);
          return new Not(notExpression);
        case AND:
          And andExpr = (And) expr;
          Expression leftAnd = visitEvaluator(andExpr.left(), caseSensitive);
          Expression rightAnd = visitEvaluator(andExpr.right(), caseSensitive);
          return new And(leftAnd, rightAnd);
        case OR:
          Or orExpr = (Or) expr;
          Expression leftOr = visitEvaluator(orExpr.left(), caseSensitive);
          Expression rightOr = visitEvaluator(orExpr.right(), caseSensitive);
          return new Or(leftOr, rightOr);
        default:
          throw new UnsupportedOperationException("Unknown operation: " + expr.op());
      }
    }
  }

  private Expression getBoundExpression(Expression expr, BoundPredicate<?> boundPre) {
    if (!(boundPre.term() instanceof BoundReference)) {
      throw new ValidationException("%s does not support non-reference: %s", this, boundPre.term());
    }
    BoundReference<?> term = (BoundReference<?>) boundPre.term();
    if (!ids.contains(term.fieldId())) {
      return True.INSTANCE;
    }
    return expr;
  }
}
