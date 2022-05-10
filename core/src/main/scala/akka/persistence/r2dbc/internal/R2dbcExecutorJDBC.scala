/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal

import akka.Done
import akka.actor.typed.{ ActorSystem, DispatcherSelector }
import akka.annotation.InternalStableApi
import org.slf4j.Logger

import java.sql.{ Connection, PreparedStatement, ResultSet, Statement }
import javax.sql.DataSource
import scala.collection.immutable.VectorBuilder
import scala.collection.{ immutable, mutable }
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

/**
 * INTERNAL API
 */
@InternalStableApi object R2dbcExecutorJDBC {
  /*final implicit class PublisherOps[T](val publisher: Publisher[T]) extends AnyVal {
    def asFuture(): Future[T] =
      Mono.from(publisher).toFuture.toScala

    def asFutureDone(): Future[Done] = {
      val mono: Mono[Done] = Mono.from(publisher).map(_ => Done)
      mono.defaultIfEmpty(Done).toFuture.toScala
    }
  }*/

  def updateOneInTx(stmt: Statement)(implicit ec: ExecutionContext): Future[Int] =
    Future(stmt.executeBatch().sum).recover { case t: Throwable =>
      t.printStackTrace()
      throw t
    }

  def updateBatchInTx(stmt: Statement)(implicit ec: ExecutionContext): Future[Int] =
    Future(stmt.executeBatch().sum).recover { case t: Throwable =>
      t.printStackTrace()
      throw t
    }

  def updateInTx(statements: immutable.IndexedSeq[PreparedStatement])(implicit
      ec: ExecutionContext): Future[immutable.IndexedSeq[Int]] =
    // connection not intended for concurrent calls, make sure statements are executed one at a time
    statements.foldLeft(Future.successful(IndexedSeq.empty[Int])) { (acc, stmt) =>
      acc.flatMap { seq =>
        Future(stmt.executeUpdate()).map(seq :+ _).recover { case t: Throwable =>
          t.printStackTrace()
          throw t
        }
      }
    }

  def selectOneInTx[A](statement: PreparedStatement, mapRow: ResultSet => A)(implicit
      ec: ExecutionContext): Future[Option[A]] =
    selectInTx(statement, mapRow).map(_.headOption)

  def selectInTx[A](statement: PreparedStatement, mapRow: ResultSet => A)(implicit
      ec: ExecutionContext): Future[immutable.IndexedSeq[A]] =
    Future(statement.executeQuery())
      .map { result =>
        val res = new VectorBuilder[A] // FIXME: can we provide hint?
        while (result.next()) res += mapRow(result)
        res.result()
      }
      .recover { case t: Throwable =>
        t.printStackTrace()
        throw t
      }
}

/**
 * INTERNAL API:
 */
@InternalStableApi
class R2dbcExecutorJDBC(val connectionFactory: DataSource, log: Logger, logDbCallsExceeding: FiniteDuration)(implicit
    //ec: ExecutionContext,
    system: ActorSystem[_]) {
  val dbDispatcher: ExecutionContext =
    system.dispatchers.lookup(DispatcherSelector.fromConfig("akka.persistence.r2dbc.db-dispatcher"))

  import R2dbcExecutorJDBC._

  private val logDbCallsExceedingMicros = logDbCallsExceeding.toMicros
  private val logDbCallsExceedingEnabled = logDbCallsExceedingMicros >= 0

  private def nanoTime(): Long =
    if (logDbCallsExceedingEnabled) System.nanoTime() else 0L

  private def durationInMicros(startTime: Long): Long =
    (nanoTime() - startTime) / 1000

  private def getConnection(logPrefix: String): Future[Connection] = {
    val startTime = nanoTime()
    Future(connectionFactory.getConnection)(dbDispatcher) // FIXME: need a cleverer way to wait for connection
      .map { connection =>
        val durationMicros = durationInMicros(startTime)
        if (durationMicros >= logDbCallsExceedingMicros)
          log.info("{} - getConnection took [{}] µs", logPrefix, durationMicros)
        connection
      }(ExecutionContext.parasitic)
  }

  /**
   * Run DDL statement with auto commit.
   */
  def executeDdl(logPrefix: String)(statement: Connection => Statement): Future[Done] =
    withAutoCommitConnection(logPrefix) { connection =>
      val stmt = statement(connection)
      Future {
        stmt.executeBatch()
        Done
      }(dbDispatcher)
    }

  /**
   * Run DDL statements in the same transaction.
   */
  def executeDdls(logPrefix: String)(statementFactory: Connection => immutable.IndexedSeq[Statement]): Future[Done] =
    withConnection(logPrefix) { connection =>
      val stmts = statementFactory(connection)
      // connection not intended for concurrent calls, make sure statements are executed one at a time
      stmts.foldLeft(Future.successful[Done](Done)) { (acc, stmt) =>
        acc.flatMap { _ =>
          Future { stmt.executeBatch(); Done }(dbDispatcher)
        }(dbDispatcher)
      }
    }

  /**
   * One update statement with auto commit.
   */
  def updateOne(logPrefix: String)(statementFactory: Connection => Statement): Future[Int] =
    withAutoCommitConnection(logPrefix) { connection =>
      updateOneInTx(statementFactory(connection))(dbDispatcher)
    }

  /**
   * Update statement that is constructed by several statements combined with `add()`.
   */
  def updateInBatch(logPrefix: String)(statementFactory: Connection => PreparedStatement): Future[Int] =
    withConnection(logPrefix) { connection =>
      connection.setAutoCommit(false)
      updateBatchInTx(statementFactory(connection))(dbDispatcher)
    }

  /**
   * Several update statements in the same transaction.
   */
  def update(logPrefix: String)(
      statementsFactory: Connection => immutable.IndexedSeq[PreparedStatement]): Future[immutable.IndexedSeq[Int]] =
    withConnection(logPrefix) { connection =>
      connection.setAutoCommit(false)
      updateInTx(statementsFactory(connection))(dbDispatcher)
    }

  /**
   * One update statement with auto commit and return mapped result. For example with Postgres:
   * {{{
   * INSERT INTO foo(name) VALUES ('bar') returning db_timestamp
   * }}}
   */
  def updateOneReturning[A](
      logPrefix: String)(statementFactory: Connection => PreparedStatement, mapRow: ResultSet => A): Future[A] = {
    withAutoCommitConnection(logPrefix) { connection =>
      val stmt = statementFactory(connection)
      selectOneInTx(stmt, mapRow)(dbDispatcher).map(_.get)(dbDispatcher)
    /*stmt.execute().asFuture().flatMap { result =>
        Mono
          .from[A](result.map((row, _) => mapRow(row)))
          .asFuture()
      }*/
    }
  }

  /**
   * Update statement that is constructed by several statements combined with `add()`. Returns the mapped result of all
   * rows. For example with Postgres:
   * {{{
   * INSERT INTO foo(name) VALUES ('bar') returning db_timestamp
   * }}}
   */
  /*def updateInBatchReturning[A](logPrefix: String)(
      statementFactory: Connection => PreparedStatement,
      mapRow: ResultSet => A): Future[immutable.IndexedSeq[A]] = {
    //import scala.jdk.CollectionConverters._
    withConnection(logPrefix) { connection =>
      val stmt = statementFactory(connection)
      //selectInTx(stmt, mapRow)(dbDispatcher)
      Future(stmt.executeBatch())(dbDispatcher).map { _ =>
        mapRow(stmt.getResultSet)
      }(dbDispatcher)
    }.recover { case t: Throwable =>
      t.printStackTrace()
      throw t
    }(dbDispatcher)
  }*/

  def selectOne[A](
      logPrefix: String)(statement: Connection => PreparedStatement, mapRow: ResultSet => A): Future[Option[A]] = {
    select(logPrefix)(statement, mapRow).map(_.headOption)(dbDispatcher)
  }

  def select[A](logPrefix: String)(
      statement: Connection => PreparedStatement,
      mapRow: ResultSet => A): Future[immutable.IndexedSeq[A]] = {
    getConnection(logPrefix).flatMap { connection =>
      val startTime = nanoTime()
      val mappedRows =
        try {
          val boundStmt = statement(connection)
          selectInTx(boundStmt, mapRow)(dbDispatcher)
        } catch {
          case NonFatal(exc) =>
            // thrown from statement function
            Future.failed(exc)
        }

      mappedRows.failed.foreach { exc =>
        log.debug("{} - Select failed: {}", logPrefix, exc)
        connection.close()
      }(dbDispatcher)

      mappedRows.map { r =>
        val durationMicros = durationInMicros(startTime)
        if (durationMicros >= logDbCallsExceedingMicros)
          log.info("{} - Selected [{}] rows in [{}] µs", logPrefix, r.size, durationMicros)
        r
      }(dbDispatcher)

    }(dbDispatcher)
  }

  /**
   * Runs the passed function in using a Connection that's participating on a transaction Transaction is commit at the
   * end or rolled back in case of failures.
   */
  def withConnection[A](logPrefix: String)(fun: Connection => Future[A]): Future[A] = {
    getConnection(logPrefix).flatMap { connection =>
      val startTime = nanoTime()
      connection.setAutoCommit(false)
      val result = fun(connection)

      result.failed.foreach { exc =>
        if (log.isDebugEnabled())
          log.debug("{} - DB call failed: {}", logPrefix, exc.toString)
        // ok to rollback async like this, or should it be before completing the returned Future?
        connection.rollback()
        connection.close()
      }(dbDispatcher)

      result.map { r =>
        connection.commit()
        connection.close()
        val durationMicros = durationInMicros(startTime)
        if (durationMicros >= logDbCallsExceedingMicros)
          log.info("{} - DB call completed in [{}] µs", logPrefix, durationMicros)
        r
      }(dbDispatcher)

    }(dbDispatcher)
  }

  /**
   * Runs the passed function in using a Connection with auto-commit enable (non-transactional).
   */
  def withAutoCommitConnection[A](logPrefix: String)(fun: Connection => Future[A]): Future[A] = {
    getConnection(logPrefix).flatMap { connection =>
      val startTime = nanoTime()
      connection.setAutoCommit(true)
      val result = fun(connection)

      result.failed.foreach { exc =>
        log.debug("{} - DB call failed: {}", logPrefix, exc)
        // auto-commit so nothing to rollback
        connection.close()
      }(dbDispatcher)

      result.map { r =>
        connection.close()
        val durationMicros = durationInMicros(startTime)
        if (durationMicros >= logDbCallsExceedingMicros)
          log.info("{} - DB call completed [{}] in [{}] µs", logPrefix, r, durationMicros)
        r
      }(dbDispatcher)
    }(dbDispatcher)
  }
}
