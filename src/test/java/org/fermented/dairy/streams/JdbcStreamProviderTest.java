package org.fermented.dairy.streams;

import org.fermented.dairy.streams.test.entity.StreamTestClass;
import org.fermented.dairy.streams.test.entity.TestRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@DisplayName("Integration tests for JdbcStreamProvider")
@ExtendWith(MockitoExtension.class)
class JdbcStreamProviderTest {

    @Mock
    Connection connection;

    @Mock
    Statement statement;

    @Mock
    ResultSet resultSet;

    @DisplayName("given result set with rows then map to record and stream results and verify close called on closable resources")
    @Test
    void givenResultSetWithRowsThenMapToRecordAndStreamResultsAndVerifyCloseCalledOnClosableResources() throws SQLException {

        when(resultSet.next()).thenReturn(true, true, false);
        when(resultSet.getString("name")).thenReturn("theFirstName", "theSecondName");
        when(resultSet.getLong("number")).thenReturn(1234L, 5678L);

        List<TestRecord> recordList = StreamTestClass.fetchStreamTryWithResource(connection, statement, resultSet);

        assertAll(
                "Validate result list",
                () -> assertEquals(2, recordList.size()),
                () -> assertEquals(new TestRecord("theFirstName", 1234L), recordList.get(0)),
                () -> assertEquals(new TestRecord("theSecondName", 5678L), recordList.get(1))
        );

        verify(connection).close();
        verify(statement).close();
        verify(resultSet).close();
    }

    @DisplayName("given result set with rows then map to record and stream results and verify close called on closable resources (no try with resource)")
    @Test
    void givenResultSetWithRowsThenMapToRecordAndStreamResultsAndVerifyCloseCalledOnClosableResourcesNoTryWithResource() throws SQLException {

        when(resultSet.next()).thenReturn(true, true, false);
        when(resultSet.getString("name")).thenReturn("theFirstName", "theSecondName");
        when(resultSet.getLong("number")).thenReturn(1234L, 5678L);

        List<TestRecord> recordList = StreamTestClass.fetchStreamNoTryWithResource(connection, statement, resultSet);

        assertAll(
                "Validate result list",
                () -> assertEquals(2, recordList.size()),
                () -> assertEquals(new TestRecord("theFirstName", 1234L), recordList.get(0)),
                () -> assertEquals(new TestRecord("theSecondName", 5678L), recordList.get(1))
        );

        verify(connection).close();
        verify(statement).close();
        verify(resultSet).close();
    }

    @DisplayName("given result set with rows then map to record then Strings and stream results and verify close called on closable resources (no try with resource)")
    @Test
    void givenResultSetWithRowsThenMapToRecordThenStringsAndStreamResultsAndVerifyCloseCalledOnClosableResourcesNoTryWithResource() throws SQLException {

        when(resultSet.next()).thenReturn(true, true, false);
        when(resultSet.getString("name")).thenReturn("theFirstName", "theSecondName");
        when(resultSet.getLong("number")).thenReturn(1234L, 5678L);

        List<String> recordList = StreamTestClass.fetchStreamNoTryWithResourceStrings(connection, statement, resultSet);

        assertAll(
                "Validate result list",
                () -> assertEquals(2, recordList.size()),
                () -> assertEquals("theFirstName:1234", recordList.get(0)),
                () -> assertEquals("theSecondName:5678", recordList.get(1))
        );

        verify(connection).close();
        verify(statement).close();
        verify(resultSet).close();
    }

    @DisplayName("given result set with rows then map to record then Strings and iterate results and verify close called on closable resources (no try with resource)")
    @Test
    void givenResultSetWithRowsThenMapToRecordThenStringsAndIterateResultsAndVerifyCloseCalledOnClosableResourcesNoTryWithResource() throws SQLException {

        when(resultSet.next()).thenReturn(true, true, false);
        when(resultSet.getString("name")).thenReturn("theFirstName", "theSecondName");
        when(resultSet.getLong("number")).thenReturn(1234L, 5678L);

        List<String> recordList = StreamTestClass.fetchStreamNoTryWithResourceStringsIterated(connection, statement, resultSet);

        assertAll(
                "Validate result list",
                () -> assertEquals(2, recordList.size()),
                () -> assertEquals("theFirstName:1234", recordList.get(0)),
                () -> assertEquals("theSecondName:5678", recordList.get(1))
        );

        verify(connection).close();
        verify(statement).close();
        verify(resultSet).close();
    }

    @DisplayName("given result set with no rows then map to record and stream results and verify close called on closable resources")
    @Test
    void givenResultSetWithNoRowsThenMapToRecordAndStreamResultsAndVerifyCloseCalledOnClosableResources() throws SQLException {

        when(resultSet.next()).thenReturn(false);

        try (Stream<TestRecord> recordStream = JdbcStreamProvider.getStream(connection, statement, resultSet, rs -> {
            try {
                return new TestRecord(rs.getString("name"), rs.getLong("number"));
            } catch (SQLException e) {
                throw new RuntimeSQLException(e);
            }
        })) {
            List<TestRecord> recordList = recordStream.toList();
            assertTrue(recordList.isEmpty());
        }

        verify(resultSet, never()).getString("name");
        verify(resultSet, never()).getLong("number");
        verify(connection).close();
        verify(statement).close();
        verify(resultSet).close();
    }

    @DisplayName("given result set with rows where exception caught when mapping then verify close called on closable resources")
    @Test
    void givenResultSetWithRowsWhereExceptionCaughtWhenMappingThenVerifyCloseCalledOnClosableResources() throws SQLException {

        when(resultSet.next()).thenReturn(true, true);
        when(resultSet.getString("name")).thenReturn("theFirstName", "theSecondName");
        when(resultSet.getLong("number")).thenReturn(1234L, 5678L);
        try(Stream<TestRecord> stream = JdbcStreamProvider.getStream(
                connection, statement, resultSet, rs -> {
                    try {
                        String name = rs.getString("name");
                        Long number = rs.getLong("number");
                        if ("theSecondName".equals(name)) {
                            throw new RuntimeException("closeables must still close");
                        }
                        return new TestRecord(name, number);
                    } catch (SQLException e) {
                        throw new RuntimeSQLException(e);
                    }
                })) {

            RuntimeException rEx = assertThrows(RuntimeException.class, stream::toList);

            assertEquals("closeables must still close", rEx.getMessage());
        }


        verify(connection).close();
        verify(statement).close();
        verify(resultSet).close();
    }

    @DisplayName("given result set with rows where exception caught when mapping then verify close called on closable resources (no try finally)")
    @Test
    void givenResultSetWithRowsWhereExceptionCaughtWhenMappingThenVerifyCloseCalledOnClosableResourcesNoTryFinally() throws SQLException {

        when(resultSet.next()).thenReturn(true, true);
        when(resultSet.getString("name")).thenReturn("theFirstName", "theSecondName");
        when(resultSet.getLong("number")).thenReturn(1234L, 5678L);
        Stream<TestRecord> stream = JdbcStreamProvider.getStream(
                connection, statement, resultSet, rs -> {
                    try {
                        String name = rs.getString("name");
                        Long number = rs.getLong("number");
                        if ("theSecondName".equals(name)) {
                            throw new RuntimeException("closeables must still close");
                        }
                        return new TestRecord(name, number);
                    } catch (SQLException e) {
                        throw new RuntimeSQLException(e);
                    }
                });

            RuntimeException rEx = assertThrows(RuntimeException.class, stream::toList);

            assertEquals("closeables must still close", rEx.getMessage());


        verify(connection).close();
        verify(statement).close();
        verify(resultSet).close();
    }

    @DisplayName("given result set with rows where exception caught when mapping and iterating then verify close called on closable resources (no try finally)")
    @Test
    void givenResultSetWithRowsWhereExceptionCaughtWhenMappingAndIteratingThenVerifyCloseCalledOnClosableResourcesNoTryFinally() throws SQLException {

        when(resultSet.next()).thenReturn(true, true);
        when(resultSet.getString("name")).thenReturn("theFirstName", "theSecondName");
        when(resultSet.getLong("number")).thenReturn(1234L, 5678L);
        Iterator<TestRecord> it = JdbcStreamProvider.getStream(
                connection, statement, resultSet, rs -> {
                    try {
                        String name = rs.getString("name");
                        Long number = rs.getLong("number");
                        if ("theSecondName".equals(name)) {
                            throw new RuntimeException("closeables must still close");
                        }
                        return new TestRecord(name, number);
                    } catch (SQLException e) {
                        throw new RuntimeSQLException(e);
                    }
                }).iterator();

        try{//NOSONAR java:S5778, this is deliberate
            while(it.hasNext()){
                it.next();
            }
            fail();//Should not get here
        } catch (RuntimeException rEx){
            assertEquals("closeables must still close", rEx.getMessage());
        }

        verify(connection).close();
        verify(statement).close();
        verify(resultSet).close();
    }
}