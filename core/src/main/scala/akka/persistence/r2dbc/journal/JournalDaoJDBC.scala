/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.journal

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.Persistence
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.R2dbcExecutorJDBC
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.typed.PersistenceId

import java.sql.{ PreparedStatement, Timestamp, Types }
import java.time.Instant
import javax.sql.DataSource
import scala.concurrent.{ ExecutionContext, Future }

/**
 * INTERNAL API
 *
 * Class for doing db interaction outside of an actor to avoid mistakes in future callbacks
 */
@InternalApi
private[r2dbc] class JournalDaoJDBC(journalSettings: R2dbcSettings, connectionFactory: DataSource)(implicit
    ec: ExecutionContext,
    system: ActorSystem[_]) {

  import JournalDao.{ log, SerializedJournalRow }

  private val persistenceExt = Persistence(system)

  private val r2dbcExecutor = new R2dbcExecutorJDBC(connectionFactory, log, journalSettings.logDbCallsExceeding)(system)

  private val journalTable = journalSettings.journalTableWithSchema

  private val (insertEventWithParameterTimestampSql, insertEventWithTransactionTimestampSql) = {
    val baseSql =
      s"INSERT INTO $journalTable " +
      "(slice, entity_type, persistence_id, seq_nr, writer, adapter_manifest, event_ser_id, event_ser_manifest, event_payload, tags, meta_ser_id, meta_ser_manifest, meta_payload, db_timestamp) " +
      "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "

    // The subselect of the db_timestamp of previous seqNr for same pid is to ensure that db_timestamp is
    // always increasing for a pid (time not going backwards).
    // TODO we could skip the subselect when inserting seqNr 1 as a possible optimization
    def timestampSubSelect =
      s"(SELECT db_timestamp + '1 microsecond'::interval FROM $journalTable " +
      "WHERE persistence_id = ? AND seq_nr = ?)"

    val insertEventWithParameterTimestampSql = {
      if (journalSettings.dbTimestampMonotonicIncreasing)
        s"$baseSql ?) RETURNING db_timestamp"
      else
        s"$baseSql GREATEST(?, $timestampSubSelect)) RETURNING db_timestamp"
    }

    val insertEventWithTransactionTimestampSql = {
      if (journalSettings.dbTimestampMonotonicIncreasing)
        s"$baseSql transaction_timestamp()) RETURNING db_timestamp"
      else
        s"$baseSql GREATEST(transaction_timestamp(), $timestampSubSelect)) RETURNING db_timestamp"
    }

    (insertEventWithParameterTimestampSql, insertEventWithTransactionTimestampSql)
  }

  private val selectHighestSequenceNrSql = s"""
    SELECT MAX(seq_nr) from $journalTable
    WHERE persistence_id = ? AND seq_nr >= ?"""

  private val deleteEventsSql = s"""
    DELETE FROM $journalTable
    WHERE persistence_id = ? AND seq_nr <= ?"""
  private val insertDeleteMarkerSql = s"""
    INSERT INTO $journalTable
    (slice, entity_type, persistence_id, seq_nr, db_timestamp, writer, adapter_manifest, event_ser_id, event_ser_manifest, event_payload, deleted)
    VALUES (?, ?, ?, ?, transaction_timestamp(), ?, ?, ?, ?, ?, ?)"""

  /**
   * All events must be for the same persistenceId.
   *
   * The returned timestamp should be the `db_timestamp` column and it is used in published events when that feature is
   * enabled.
   *
   * Note for implementing future database dialects: If a database dialect can't efficiently return the timestamp column
   * it can return `JournalDao.EmptyDbTimestamp` when the pub-sub feature is disabled. When enabled it would have to use
   * a select (in same transaction).
   */
  def writeEvents(events: Seq[SerializedJournalRow]): Future[Instant] = {
    require(events.nonEmpty)

    // it's always the same persistenceId for all events
    val persistenceId = events.head.persistenceId
    val previousSeqNr = events.head.seqNr - 1

    // The MigrationTool defines the dbTimestamp to preserve the original event timestamp
    val useTimestampFromDb = events.head.dbTimestamp == Instant.EPOCH

    def bind(stmt: PreparedStatement, write: SerializedJournalRow): PreparedStatement = {
      import stmt._
      setInt(1, write.slice)
      setString(2, write.entityType)
      setString(3, write.persistenceId)
      setLong(4, write.seqNr)
      setString(5, write.writerUuid)
      setString(6, "") // FIXME event adapter
      setInt(7, write.serId)
      setString(8, write.serManifest)
      setBytes(9, write.payload.get)

      if (write.tags.isEmpty)
        setNull(10, Types.ARRAY)
      else
        ??? //FIXME setArray(9, java.sql.Array(write.tags.toArray)

      // optional metadata
      write.metadata match {
        case Some(m) =>
          setInt(11, m.serId)
          setString(12, m.serManifest)
          setBytes(13, m.payload)
        case None =>
          setNull(11, Types.INTEGER)
          setNull(12, Types.VARCHAR)
          setNull(13, Types.BINARY)
      }

      if (useTimestampFromDb) {
        if (!journalSettings.dbTimestampMonotonicIncreasing) {
          setString(14, write.persistenceId)
          setLong(15, previousSeqNr)
        }

      } else {
        if (journalSettings.dbTimestampMonotonicIncreasing) {
          setTimestamp(14, Timestamp.from(write.dbTimestamp))
        } else {
          setTimestamp(14, Timestamp.from(write.dbTimestamp))
          setString(15, write.persistenceId)
          setLong(16, previousSeqNr)
        }
      }

      stmt
    }

    val insertSql =
      if (useTimestampFromDb) insertEventWithTransactionTimestampSql
      else insertEventWithParameterTimestampSql

    val totalEvents = events.size
    if (totalEvents == 1) {
      val result = r2dbcExecutor.updateOneReturning(s"insert [$persistenceId]")(
        connection => bind(connection.prepareStatement(insertSql), events.head),
        row => row.getTimestamp(1).toInstant)
      if (log.isDebugEnabled())
        result.foreach { _ =>
          log.debug("Wrote [{}] events for persistenceId [{}]", 1, events.head.persistenceId)
        }
      result
    } else {
      val result = r2dbcExecutor.updateInBatch(s"batch insert [$persistenceId], [$totalEvents] events")(connection =>
        events.foldLeft(connection.prepareStatement(insertSql)) { (stmt, write) =>
          val next = bind(stmt, write)
          stmt.addBatch()
          next
        })
      if (log.isDebugEnabled())
        result.foreach { _ =>
          log.debug("Wrote [{}] events for persistenceId [{}]", 1, events.head.persistenceId)
        }

      //result.map(_.head)(ExecutionContext.parasitic)
      result.flatMap { _ =>
        r2dbcExecutor
          .selectOne("get last timestamp")(
            { conn =>
              val stmt = conn.prepareStatement(s"select max(db_timestamp) from $journalTable where persistence_id = ?")
              stmt.setString(1, persistenceId)
              stmt
            },
            _.getTimestamp(1).toInstant)
          .map(_.get)
      }
    }
  }

  def readHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    val result = r2dbcExecutor
      .select(s"select highest seqNr [$persistenceId]")(
        { connection =>
          val stmt = connection.prepareStatement(selectHighestSequenceNrSql)

          stmt.setString(1, persistenceId)
          stmt.setLong(2, fromSequenceNr)
          stmt
        },
        _.getLong(1))
      .map(r => if (r.isEmpty) 0L else r.head)(ExecutionContext.parasitic)

    if (log.isDebugEnabled)
      result.foreach(seqNr => log.debug("Highest sequence nr for persistenceId [{}]: [{}]", persistenceId, seqNr))

    result
  }

  def deleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    val entityType = PersistenceId.extractEntityType(persistenceId)
    val slice = persistenceExt.sliceForPersistenceId(persistenceId)

    val deleteMarkerSeqNrFut =
      if (toSequenceNr == Long.MaxValue)
        readHighestSequenceNr(persistenceId, 0L)
      else
        Future.successful(toSequenceNr)

    deleteMarkerSeqNrFut.flatMap { deleteMarkerSeqNr =>
      def bindDeleteMarker(stmt: PreparedStatement): PreparedStatement = {
        import stmt._
        setInt(1, slice)
        setString(2, entityType)
        setString(3, persistenceId)
        setLong(4, deleteMarkerSeqNr)
        setString(5, "")
        setString(6, "")
        setInt(7, 0)
        setString(8, "")
        setBytes(9, Array.emptyByteArray)
        setBoolean(10, true)
        stmt
      }

      val result = r2dbcExecutor.update(s"delete [$persistenceId]") { connection =>
        val deleteStmt = {
          val stmt = connection.prepareStatement(deleteEventsSql)
          stmt.setString(1, persistenceId)
          stmt.setLong(2, toSequenceNr)
          stmt
        }
        val insertDeleteStatement = bindDeleteMarker(connection.prepareStatement(insertDeleteMarkerSql))
        Vector(deleteStmt, insertDeleteStatement)
      }

      if (log.isDebugEnabled)
        result.foreach(updatedRows =>
          log.debug("Deleted [{}] events for persistenceId [{}]", updatedRows.head, persistenceId))

      result.map(_ => ())(ExecutionContext.parasitic)
    }
  }

}
