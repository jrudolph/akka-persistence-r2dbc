/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.query.scaladsl

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.BySliceQuery.Buckets
import akka.persistence.r2dbc.internal.BySliceQuery.Buckets.Bucket
import akka.persistence.r2dbc.internal.{ BySliceQuery, R2dbcExecutor, R2dbcExecutorJDBC }
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.r2dbc.journal.JournalDao
import akka.persistence.r2dbc.journal.JournalDao.SerializedJournalRow
import akka.stream.scaladsl.Source
import io.r2dbc.spi.ConnectionFactory

import java.sql.Timestamp
import java.time.Instant
import javax.sql.DataSource
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.{ Duration, FiniteDuration }

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] class QueryDaoJDBC(settings: R2dbcSettings, connectionFactory: DataSource)(implicit
    //ec: ExecutionContext,
    system: ActorSystem[_])
    extends BySliceQuery.Dao[SerializedJournalRow] {
  import system.executionContext
  import JournalDao.readMetadata
  import QueryDao.log

  private val journalTable = settings.journalTableWithSchema

  private val currentDbTimestampSql =
    "SELECT transaction_timestamp() AS db_timestamp"

  private def eventsBySlicesRangeSql(
      toDbTimestampParam: Boolean,
      behindCurrentTime: FiniteDuration,
      backtracking: Boolean): String = {

    def toDbTimestampParamCondition =
      if (toDbTimestampParam) "AND db_timestamp <= ?" else ""

    def behindCurrentTimeIntervalCondition =
      if (behindCurrentTime > Duration.Zero)
        s"AND db_timestamp < transaction_timestamp() - interval '${behindCurrentTime.toMillis} milliseconds'"
      else ""

    val selectColumns = {
      if (backtracking)
        "SELECT slice, persistence_id, seq_nr, db_timestamp, statement_timestamp() AS read_db_timestamp "
      else
        "SELECT slice, persistence_id, seq_nr, db_timestamp, statement_timestamp() AS read_db_timestamp, event_ser_id, event_ser_manifest, event_payload, meta_ser_id, meta_ser_manifest, meta_payload "
    }

    s"""
      $selectColumns
      FROM $journalTable
      WHERE entity_type = ?
      AND slice BETWEEN ? AND ?
      AND db_timestamp >= ? $toDbTimestampParamCondition $behindCurrentTimeIntervalCondition
      AND deleted = false
      ORDER BY db_timestamp, seq_nr
      LIMIT ?"""
  }

  private val selectBucketsSql = s"""
    SELECT extract(EPOCH from db_timestamp)::BIGINT / 10 AS bucket, count(*) AS count
    FROM $journalTable
    WHERE entity_type = ?
    AND slice BETWEEN ? AND ?
    AND db_timestamp >= ? AND db_timestamp <= ?
    AND deleted = false
    GROUP BY bucket ORDER BY bucket LIMIT ?
    """

  private val selectTimestampOfEventSql = s"""
    SELECT db_timestamp FROM $journalTable
    WHERE persistence_id = ? AND seq_nr = ? AND deleted = false"""

  private val selectOneEventSql = s"""
    SELECT slice, entity_type, db_timestamp, statement_timestamp() AS read_db_timestamp, event_ser_id, event_ser_manifest, event_payload, meta_ser_id, meta_ser_manifest, meta_payload
    FROM $journalTable
    WHERE persistence_id = ? AND seq_nr = ? AND deleted = false"""

  private val selectEventsSql = s"""
    SELECT slice, entity_type, persistence_id, seq_nr, db_timestamp, statement_timestamp() AS read_db_timestamp, event_ser_id, event_ser_manifest, event_payload, writer, adapter_manifest, meta_ser_id, meta_ser_manifest, meta_payload
    from $journalTable
    WHERE persistence_id = ? AND seq_nr >= ? AND seq_nr <= ?
    AND deleted = false
    ORDER BY seq_nr
    LIMIT ?"""

  private val allPersistenceIdsSql =
    s"SELECT DISTINCT(persistence_id) from $journalTable ORDER BY persistence_id LIMIT ?"

  private val allPersistenceIdsAfterSql =
    s"SELECT DISTINCT(persistence_id) from $journalTable WHERE persistence_id > ? ORDER BY persistence_id LIMIT ?"

  private val r2dbcExecutor = new R2dbcExecutorJDBC(connectionFactory, log, settings.logDbCallsExceeding)(system)

  def currentDbTimestamp(): Future[Instant] = {
    r2dbcExecutor
      .selectOne("select current db timestamp")(
        connection => connection.prepareStatement(currentDbTimestampSql),
        row => row.getTimestamp("db_timestamp").toInstant)
      .map {
        case Some(time) => time
        case None       => throw new IllegalStateException(s"Expected one row for: $currentDbTimestampSql")
      }
  }

  def rowsBySlices(
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      fromTimestamp: Instant,
      toTimestamp: Option[Instant],
      behindCurrentTime: FiniteDuration,
      backtracking: Boolean): Source[SerializedJournalRow, NotUsed] = {
    val result = r2dbcExecutor.select(s"select eventsBySlices [$minSlice - $maxSlice]")(
      connection => {
        val stmt = connection
          .prepareStatement(
            eventsBySlicesRangeSql(toDbTimestampParam = toTimestamp.isDefined, behindCurrentTime, backtracking))

        import stmt._
        setString(1, entityType)
        setInt(2, minSlice)
        setInt(3, maxSlice)
        setTimestamp(4, Timestamp.from(fromTimestamp))
        toTimestamp match {
          case Some(until) =>
            stmt.setTimestamp(5, Timestamp.from(until))
            stmt.setInt(6, settings.querySettings.bufferSize)
          case None =>
            stmt.setInt(5, settings.querySettings.bufferSize)
        }
        stmt
      },
      row =>
        if (backtracking)
          SerializedJournalRow(
            slice = row.getInt("slice"),
            entityType,
            persistenceId = row.getString("persistence_id"),
            seqNr = row.getLong("seq_nr"),
            dbTimestamp = row.getTimestamp("db_timestamp").toInstant,
            readDbTimestamp = row.getTimestamp("read_db_timestamp").toInstant,
            payload = None, // lazy loaded for backtracking
            serId = 0,
            serManifest = "",
            writerUuid = "", // not need in this query
            tags = Set.empty, // tags not fetched in queries (yet)
            metadata = None)
        else
          SerializedJournalRow(
            slice = row.getInt("slice"),
            entityType,
            persistenceId = row.getString("persistence_id"),
            seqNr = row.getLong("seq_nr"),
            dbTimestamp = row.getTimestamp("db_timestamp").toInstant,
            readDbTimestamp = row.getTimestamp("read_db_timestamp").toInstant,
            payload = Some(row.getBytes("event_payload")),
            serId = row.getInt("event_ser_id"),
            serManifest = row.getString("event_ser_manifest"),
            writerUuid = "", // not need in this query
            tags = Set.empty, // tags not fetched in queries (yet)
            metadata = readMetadata(row)))

    if (log.isDebugEnabled)
      result.foreach(rows => log.debug("Read [{}] events from slices [{} - {}]", rows.size, minSlice, maxSlice))

    Source.futureSource(result.map(Source(_))).mapMaterializedValue(_ => NotUsed)
  }

  override def countBuckets(
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      fromTimestamp: Instant,
      limit: Int): Future[Seq[Bucket]] = {

    val toTimestamp = {
      val now = Instant.now() // not important to use database time
      if (fromTimestamp == Instant.EPOCH)
        now
      else {
        // max buckets, just to have some upper bound
        val t = fromTimestamp.plusSeconds(Buckets.BucketDurationSeconds * limit + Buckets.BucketDurationSeconds)
        if (t.isAfter(now)) now else t
      }
    }

    val result = r2dbcExecutor.select(s"select bucket counts [$minSlice - $maxSlice]")(
      { connection =>
        val stmt =
          connection
            .prepareStatement(selectBucketsSql)

        import stmt._
        setString(1, entityType)
        setInt(2, minSlice)
        setInt(3, maxSlice)
        setTimestamp(4, Timestamp.from(fromTimestamp))
        setTimestamp(5, Timestamp.from(toTimestamp))
        setInt(6, limit)
        stmt
      },
      row => {
        val bucketStartEpochSeconds = row.getLong("bucket") * 10
        val count = row.getLong("count")
        Bucket(bucketStartEpochSeconds, count)
      })

    if (log.isDebugEnabled)
      result.foreach(rows => log.debug("Read [{}] bucket counts from slices [{} - {}]", rows.size, minSlice, maxSlice))

    result
  }

  /**
   * Events are append only
   */
  override def countBucketsMayChange: Boolean = false

  def timestampOfEvent(persistenceId: String, seqNr: Long): Future[Option[Instant]] = {
    r2dbcExecutor.selectOne("select timestampOfEvent")(
      { connection =>
        val stmt = connection
          .prepareStatement(selectTimestampOfEventSql)

        stmt.setString(1, persistenceId)
        stmt.setLong(2, seqNr)
        stmt
      },
      row => row.getTimestamp("db_timestamp").toInstant)
  }

  def loadEvent(persistenceId: String, seqNr: Long): Future[Option[SerializedJournalRow]] =
    r2dbcExecutor.selectOne("select one event")(
      { connection =>
        val stmt = connection
          .prepareStatement(selectOneEventSql)

        stmt.setString(1, persistenceId)
        stmt.setLong(2, seqNr)
        stmt
      },
      row =>
        SerializedJournalRow(
          slice = row.getInt("slice"),
          entityType = row.getString("entity_type"),
          persistenceId,
          seqNr,
          dbTimestamp = row.getTimestamp("db_timestamp").toInstant,
          readDbTimestamp = row.getTimestamp("read_db_timestamp").toInstant,
          payload = Some(row.getBytes("event_payload")),
          serId = row.getInt("event_ser_id"),
          serManifest = row.getString("event_ser_manifest"),
          writerUuid = "", // not need in this query
          tags = Set.empty, // tags not fetched in queries (yet)
          metadata = readMetadata(row)))

  def eventsByPersistenceId(
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[SerializedJournalRow, NotUsed] = {

    val result = r2dbcExecutor.select(s"select eventsByPersistenceId [$persistenceId]")(
      { connection =>
        val stmt = connection
          .prepareStatement(selectEventsSql)

        import stmt._
        setString(1, persistenceId)
        setLong(2, fromSequenceNr)
        setLong(3, toSequenceNr)
        setInt(4, settings.querySettings.bufferSize)
        stmt
      },
      row =>
        SerializedJournalRow(
          slice = row.getInt("slice"),
          entityType = row.getString("entity_type"),
          persistenceId = row.getString("persistence_id"),
          seqNr = row.getLong("seq_nr"),
          dbTimestamp = row.getTimestamp("db_timestamp").toInstant,
          readDbTimestamp = row.getTimestamp("read_db_timestamp").toInstant,
          payload = Some(row.getBytes("event_payload")),
          serId = row.getInt("event_ser_id"),
          serManifest = row.getString("event_ser_manifest"),
          writerUuid = row.getString("writer"),
          tags = Set.empty, // tags not fetched in queries (yet)
          metadata = readMetadata(row)))

    if (log.isDebugEnabled)
      result.foreach(rows => log.debug("Read [{}] events for persistenceId [{}]", rows.size, persistenceId))

    Source.futureSource(result.map(Source(_))).mapMaterializedValue(_ => NotUsed)
  }

  def persistenceIds(afterId: Option[String], limit: Long): Source[String, NotUsed] = {
    val result = r2dbcExecutor.select(s"select persistenceIds")(
      connection =>
        afterId match {
          case Some(after) =>
            val stmt = connection
              .prepareStatement(allPersistenceIdsAfterSql)

            stmt.setString(1, after)
            stmt.setLong(2, limit)

            stmt
          case None =>
            val stmt = connection
              .prepareStatement(allPersistenceIdsSql)
            stmt.setLong(1, limit)
            stmt
        },
      row => row.getString("persistence_id"))

    if (log.isDebugEnabled)
      result.foreach(rows => log.debug("Read [{}] persistence ids", rows.size))

    Source.futureSource(result.map(Source(_))).mapMaterializedValue(_ => NotUsed)
  }

}
