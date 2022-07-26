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

package org.apache.spark.sql.catalyst.parser.extensions

import java.util.Locale
import org.antlr.v4.runtime._
import org.antlr.v4.runtime.misc.Interval
import org.antlr.v4.runtime.tree.ParseTree
import org.antlr.v4.runtime.tree.TerminalNode
import org.apache.commons.lang3.StringUtils
import org.apache.iceberg.DistributionMode
import org.apache.iceberg.NullOrder
import org.apache.iceberg.SortDirection
import org.apache.iceberg.expressions.Term
import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.parser.extensions.IcebergParserUtils.withOrigin
import org.apache.spark.sql.catalyst.parser.extensions.IcebergSqlExtensionsParser._
import org.apache.spark.sql.catalyst.plans.logical.AddPartitionField
import org.apache.spark.sql.catalyst.plans.logical.AlterBranchRefRetention
import org.apache.spark.sql.catalyst.plans.logical.AlterBranchSnapshotRetention
import org.apache.spark.sql.catalyst.plans.logical.AlterTagRefRetention
import org.apache.spark.sql.catalyst.plans.logical.CallArgument
import org.apache.spark.sql.catalyst.plans.logical.CallStatement
import org.apache.spark.sql.catalyst.plans.logical.CreateBranch
import org.apache.spark.sql.catalyst.plans.logical.CreateTag
import org.apache.spark.sql.catalyst.plans.logical.DropIdentifierFields
import org.apache.spark.sql.catalyst.plans.logical.DropPartitionField
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.NamedArgument
import org.apache.spark.sql.catalyst.plans.logical.PositionalArgument
import org.apache.spark.sql.catalyst.plans.logical.RemoveBranch
import org.apache.spark.sql.catalyst.plans.logical.RemoveTag
import org.apache.spark.sql.catalyst.plans.logical.RenameBranch
import org.apache.spark.sql.catalyst.plans.logical.ReplaceBranch
import org.apache.spark.sql.catalyst.plans.logical.ReplacePartitionField
import org.apache.spark.sql.catalyst.plans.logical.ReplaceTag
import org.apache.spark.sql.catalyst.plans.logical.SetIdentifierFields
import org.apache.spark.sql.catalyst.plans.logical.SetWriteDistributionAndOrdering
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.connector.expressions
import org.apache.spark.sql.connector.expressions.ApplyTransform
import org.apache.spark.sql.connector.expressions.FieldReference
import org.apache.spark.sql.connector.expressions.IdentityTransform
import org.apache.spark.sql.connector.expressions.LiteralValue
import org.apache.spark.sql.connector.expressions.Transform
import scala.collection.JavaConverters._

class IcebergSqlExtensionsAstBuilder(delegate: ParserInterface) extends IcebergSqlExtensionsBaseVisitor[AnyRef] {

  /**
   * Create a [[CallStatement]] for a stored procedure call.
   */
  override def visitCall(ctx: CallContext): CallStatement = withOrigin(ctx) {
    val name = ctx.multipartIdentifier.parts.asScala.map(_.getText)
    val args = ctx.callArgument.asScala.map(typedVisit[CallArgument])
    CallStatement(name, args)
  }

  /**
   * Create an ADD PARTITION FIELD logical command.
   */
  override def visitAddPartitionField(ctx: AddPartitionFieldContext): AddPartitionField = withOrigin(ctx) {
    AddPartitionField(
      typedVisit[Seq[String]](ctx.multipartIdentifier),
      typedVisit[Transform](ctx.transform),
      Option(ctx.name).map(_.getText))
  }

  /**
   * Create a DROP PARTITION FIELD logical command.
   */
  override def visitDropPartitionField(ctx: DropPartitionFieldContext): DropPartitionField = withOrigin(ctx) {
    DropPartitionField(
      typedVisit[Seq[String]](ctx.multipartIdentifier),
      typedVisit[Transform](ctx.transform))
  }

  /**
   * Create an CREATE TAG logical command.
   */
  override def visitCreateTag(ctx: CreateTagContext): CreateTag = withOrigin(ctx) {
    val tagName = ctx.identifier().getText
    val snapshotId = Option(ctx.snapshotId()).map(_.getText.toLong)
    val snapshotRefRetain =
      Option(ctx.snapshotRefRetain()).map(_.getText.toLong * timeUnit(ctx.snapshotRefRetainTimeUnit().getText))
    validateTag(tagName, snapshotId, snapshotRefRetain)

    CreateTag(typedVisit[Seq[String]](ctx.multipartIdentifier), tagName, snapshotId, snapshotRefRetain)
  }

  /**
   * Create an REPLACE TAG logical command.
   */
  override def visitReplaceTag(ctx: ReplaceTagContext): ReplaceTag = withOrigin(ctx) {
    val tagName = ctx.identifier().getText
    val snapshotId = Option(ctx.snapshotId()).map(_.getText.toLong)
    val snapshotRefRetain =
      Option(ctx.snapshotRefRetain()).map(_.getText.toLong * timeUnit(ctx.snapshotRefRetainTimeUnit().getText))
    validateTag(tagName, snapshotId, snapshotRefRetain)

    ReplaceTag(typedVisit[Seq[String]](ctx.multipartIdentifier), tagName, snapshotId, snapshotRefRetain)
  }

  /**
   * Create an REMOVE TAG logical command.
   */
  override def visitRemoveTag(ctx: RemoveTagContext): RemoveTag = withOrigin(ctx) {
    val tagName = ctx.identifier().getText
    validateTag(tagName, Option.empty[Long], Option.empty[Long])

    RemoveTag(typedVisit[Seq[String]](ctx.multipartIdentifier), ctx.identifier().getText)
  }

  /**
   * Create an ALTER TAG RETENTION logical command.
   */
  override def visitAlterTagRetention(ctx: AlterTagRetentionContext): AlterTagRefRetention = withOrigin(ctx) {
    val tagName = ctx.identifier().getText
    val snapshotRefRetain =
      Option(ctx.snapshotRefRetain()).map(_.getText.toLong * timeUnit(ctx.snapshotRefRetainTimeUnit().getText))
    validateTag(tagName, Option.empty[Long], snapshotRefRetain)

    AlterTagRefRetention(typedVisit[Seq[String]](ctx.multipartIdentifier()), tagName, snapshotRefRetain)
  }

  private def validateTag(tagName: String, snapshotId: Option[Long], snapshotRefRetain: Option[Long]): Unit = {
    if (StringUtils.isBlank(tagName)) {
      throw new IllegalArgumentException("Tag name can not be empty or null")
    }

    if (snapshotId.nonEmpty && snapshotId.get <= 0) {
      throw new IllegalArgumentException("Snapshot id must be greater than 0")
    }

    if (snapshotRefRetain.nonEmpty && snapshotRefRetain.get <= 0) {
      throw new IllegalArgumentException("Max reference age must be greater than 0")
    }
  }

  /**
   * Create an ADD BRANCH logical command.
   */
  override def visitCreateBranch(ctx: CreateBranchContext): CreateBranch = withOrigin(ctx) {
    CreateBranch(
      typedVisit[Seq[String]](ctx.multipartIdentifier),
      ctx.identifier().getText,
      Option(ctx.snapshotId()).map(_.getText.toLong),
      Option(ctx.numSnapshots()).map(_.getText.toLong),
      Option(ctx.snapshotRetain()).map(_.getText.toLong * timeUnit(ctx.snapshotRetainTimeUnit().getText)),
      Option(ctx.snapshotRefRetain()).map(_.getText.toLong * timeUnit(ctx.snapshotRefRetainTimeUnit().getText))
    )
  }

  /**
   * Create an REPLACE BRANCH logical command.
   */
  override def visitReplaceBranch(ctx: ReplaceBranchContext): ReplaceBranch = withOrigin(ctx) {
    ReplaceBranch(
      typedVisit[Seq[String]](ctx.multipartIdentifier),
      ctx.identifier().getText,
      Option(ctx.snapshotId()).map(_.getText.toLong),
      Option(ctx.numSnapshots()).map(_.getText.toLong),
      Option(ctx.snapshotRetain()).map(_.getText.toLong * timeUnit(ctx.snapshotRetainTimeUnit().getText)),
      Option(ctx.snapshotRefRetain()).map(_.getText.toLong * timeUnit(ctx.snapshotRefRetainTimeUnit().getText))
    )
  }

  /**
   * Create an REMOVE BRANCH logical command.
   */
  override def visitRemoveBranch(ctx: RemoveBranchContext): RemoveBranch = withOrigin(ctx) {
    RemoveBranch(
      typedVisit[Seq[String]](ctx.multipartIdentifier),
      ctx.identifier().getText
    )
  }

  /**
   * Create an RENAME BRANCH logical command.
   */
  override def visitRenameBranch(ctx: RenameBranchContext): RenameBranch =  withOrigin(ctx) {
    RenameBranch(
      typedVisit[Seq[String]](ctx.multipartIdentifier()),
      ctx.identifier().getText,
      ctx.newIdentifier().getText)
  }

  /**
   * Create an ALTER BRANCH RETENTION logical command.
   */
  override def visitAlterBranchRetention(ctx: AlterBranchRetentionContext): AlterBranchRefRetention = withOrigin(ctx) {
    AlterBranchRefRetention(
      typedVisit[Seq[String]](ctx.multipartIdentifier()),
      ctx.identifier().getText,
      Option(ctx.snapshotRefRetain()).map(_.getText.toLong * timeUnit(ctx.snapshotRefRetainTimeUnit().getText))
    )
  }

  /**
   * Create an ALTER BRANCH SNAPSHOT RETENTION logical command.
   */
  override def visitAlterBranchSnapshotRetention(
                                                  ctx: AlterBranchSnapshotRetentionContext
                                                ): AlterBranchSnapshotRetention = withOrigin(ctx) {
    AlterBranchSnapshotRetention(
      typedVisit[Seq[String]](ctx.multipartIdentifier()),
      ctx.identifier().getText,
      Option(ctx.numSnapshots()).map(_.getText.toLong),
      Option(ctx.snapshotRetain()).map(_.getText.toLong * timeUnit(ctx.snapshotRetainTimeUnit().getText))
    )
  }

  /**
   * Create an REPLACE PARTITION FIELD logical command.
   */
  override def visitReplacePartitionField(ctx: ReplacePartitionFieldContext): ReplacePartitionField = withOrigin(ctx) {
    ReplacePartitionField(
      typedVisit[Seq[String]](ctx.multipartIdentifier),
      typedVisit[Transform](ctx.transform(0)),
      typedVisit[Transform](ctx.transform(1)),
      Option(ctx.name).map(_.getText))
  }

  /**
   * Create an SET IDENTIFIER FIELDS logical command.
   */
  override def visitSetIdentifierFields(ctx: SetIdentifierFieldsContext): SetIdentifierFields = withOrigin(ctx) {
    SetIdentifierFields(
      typedVisit[Seq[String]](ctx.multipartIdentifier),
      ctx.fieldList.fields.asScala.map(_.getText))
  }

  /**
   * Create an DROP IDENTIFIER FIELDS logical command.
   */
  override def visitDropIdentifierFields(ctx: DropIdentifierFieldsContext): DropIdentifierFields = withOrigin(ctx) {
    DropIdentifierFields(
      typedVisit[Seq[String]](ctx.multipartIdentifier),
      ctx.fieldList.fields.asScala.map(_.getText))
  }

  /**
   * Create a [[SetWriteDistributionAndOrdering]] for changing the write distribution and ordering.
   */
  override def visitSetWriteDistributionAndOrdering(
      ctx: SetWriteDistributionAndOrderingContext): SetWriteDistributionAndOrdering = {

    val tableName = typedVisit[Seq[String]](ctx.multipartIdentifier)

    val (distributionSpec, orderingSpec) = toDistributionAndOrderingSpec(ctx.writeSpec)

    if (distributionSpec == null && orderingSpec == null) {
      throw new AnalysisException(
        "ALTER TABLE has no changes: missing both distribution and ordering clauses")
    }

    val distributionMode = if (distributionSpec != null) {
      DistributionMode.HASH
    } else if (orderingSpec.UNORDERED != null || orderingSpec.LOCALLY != null) {
      DistributionMode.NONE
    } else {
      DistributionMode.RANGE
    }

    val ordering = if (orderingSpec != null && orderingSpec.order != null) {
      orderingSpec.order.fields.asScala.map(typedVisit[(Term, SortDirection, NullOrder)])
    } else {
      Seq.empty
    }

    SetWriteDistributionAndOrdering(tableName, distributionMode, ordering)
  }

  private def toDistributionAndOrderingSpec(
      writeSpec: WriteSpecContext): (WriteDistributionSpecContext, WriteOrderingSpecContext) = {

    if (writeSpec.writeDistributionSpec.size > 1) {
      throw new AnalysisException("ALTER TABLE contains multiple distribution clauses")
    }

    if (writeSpec.writeOrderingSpec.size > 1) {
      throw new AnalysisException("ALTER TABLE contains multiple ordering clauses")
    }

    val distributionSpec = writeSpec.writeDistributionSpec.asScala.headOption.orNull
    val orderingSpec = writeSpec.writeOrderingSpec.asScala.headOption.orNull

    (distributionSpec, orderingSpec)
  }

  /**
   * Create an order field.
   */
  override def visitOrderField(ctx: OrderFieldContext): (Term, SortDirection, NullOrder) = {
    val term = Spark3Util.toIcebergTerm(typedVisit[Transform](ctx.transform))
    val direction = Option(ctx.ASC).map(_ => SortDirection.ASC)
        .orElse(Option(ctx.DESC).map(_ => SortDirection.DESC))
        .getOrElse(SortDirection.ASC)
    val nullOrder = Option(ctx.FIRST).map(_ => NullOrder.NULLS_FIRST)
        .orElse(Option(ctx.LAST).map(_ => NullOrder.NULLS_LAST))
        .getOrElse(if (direction == SortDirection.ASC) NullOrder.NULLS_FIRST else NullOrder.NULLS_LAST)
    (term, direction, nullOrder)
  }

  /**
   * Create an IdentityTransform for a column reference.
   */
  override def visitIdentityTransform(ctx: IdentityTransformContext): Transform = withOrigin(ctx) {
    IdentityTransform(FieldReference(typedVisit[Seq[String]](ctx.multipartIdentifier())))
  }

  /**
   * Create a named Transform from argument expressions.
   */
  override def visitApplyTransform(ctx: ApplyTransformContext): Transform = withOrigin(ctx) {
    val args = ctx.arguments.asScala.map(typedVisit[expressions.Expression])
    ApplyTransform(ctx.transformName.getText, args)
  }

  /**
   * Create a transform argument from a column reference or a constant.
   */
  override def visitTransformArgument(ctx: TransformArgumentContext): expressions.Expression = withOrigin(ctx) {
    val reference = Option(ctx.multipartIdentifier())
        .map(typedVisit[Seq[String]])
        .map(FieldReference(_))
    val literal = Option(ctx.constant)
        .map(visitConstant)
        .map(lit => LiteralValue(lit.value, lit.dataType))
    reference.orElse(literal)
        .getOrElse(throw new IcebergParseException(s"Invalid transform argument", ctx))
  }

  /**
   * Return a multi-part identifier as Seq[String].
   */
  override def visitMultipartIdentifier(ctx: MultipartIdentifierContext): Seq[String] = withOrigin(ctx) {
    ctx.parts.asScala.map(_.getText)
  }

  /**
   * Create a positional argument in a stored procedure call.
   */
  override def visitPositionalArgument(ctx: PositionalArgumentContext): CallArgument = withOrigin(ctx) {
    val expr = typedVisit[Expression](ctx.expression)
    PositionalArgument(expr)
  }

  /**
   * Create a named argument in a stored procedure call.
   */
  override def visitNamedArgument(ctx: NamedArgumentContext): CallArgument = withOrigin(ctx) {
    val name = ctx.identifier.getText
    val expr = typedVisit[Expression](ctx.expression)
    NamedArgument(name, expr)
  }

  override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = withOrigin(ctx) {
    visit(ctx.statement).asInstanceOf[LogicalPlan]
  }

  def visitConstant(ctx: ConstantContext): Literal = {
    delegate.parseExpression(ctx.getText).asInstanceOf[Literal]
  }

  override def visitExpression(ctx: ExpressionContext): Expression = {
    // reconstruct the SQL string and parse it using the main Spark parser
    // while we can avoid the logic to build Spark expressions, we still have to parse them
    // we cannot call ctx.getText directly since it will not render spaces correctly
    // that's why we need to recurse down the tree in reconstructSqlString
    val sqlString = reconstructSqlString(ctx)
    delegate.parseExpression(sqlString)
  }

  private def reconstructSqlString(ctx: ParserRuleContext): String = {
    ctx.children.asScala.map {
      case c: ParserRuleContext => reconstructSqlString(c)
      case t: TerminalNode => t.getText
    }.mkString(" ")
  }

  private def typedVisit[T](ctx: ParseTree): T = {
    ctx.accept(this).asInstanceOf[T]
  }

  private val timeUnit = (unit: String) => {
    unit.toUpperCase(Locale.ENGLISH) match {
      case "MONTHS" => 30 * 24 * 60 * 60 * 1000L
      case "DAYS" => 24 * 60 * 60 * 1000L
      case "HOURS" => 60 * 60 * 1000L
      case "MINUTES" => 60 * 1000L
      case _ => throw new IllegalArgumentException("Invalid time unit: " + unit)
    }
  }
}

/* Partially copied from Apache Spark's Parser to avoid dependency on Spark Internals */
object IcebergParserUtils {

  private[sql] def withOrigin[T](ctx: ParserRuleContext)(f: => T): T = {
    val current = CurrentOrigin.get
    CurrentOrigin.set(position(ctx.getStart))
    try {
      f
    } finally {
      CurrentOrigin.set(current)
    }
  }

  private[sql] def position(token: Token): Origin = {
    val opt = Option(token)
    Origin(opt.map(_.getLine), opt.map(_.getCharPositionInLine))
  }

  /** Get the command which created the token. */
  private[sql] def command(ctx: ParserRuleContext): String = {
    val stream = ctx.getStart.getInputStream
    stream.getText(Interval.of(0, stream.size() - 1))
  }
}
