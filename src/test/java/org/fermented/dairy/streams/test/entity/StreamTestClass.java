package org.fermented.dairy.streams.test.entity;

import org.fermented.dairy.streams.JdbcStreamProvider;
import org.fermented.dairy.streams.RuntimeSQLException;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

public class StreamTestClass {

    public static List<TestRecord> fetchStreamTryWithResource(Connection connection, Statement statement, ResultSet resultSet){

        try (Stream<TestRecord> recordStream = JdbcStreamProvider.getStream(connection, statement, resultSet, rs -> {
            try {
                return new TestRecord(rs.getString("name"), rs.getLong("number"));
            } catch (SQLException e) {
                throw new RuntimeSQLException(e);
            }
        })) {
            return recordStream.toList();

        }

    }

    public static List<TestRecord> fetchStreamNoTryWithResource(Connection connection, Statement statement, ResultSet resultSet){

        return JdbcStreamProvider.getStream(connection, statement, resultSet, rs -> {
            try {
                return new TestRecord(rs.getString("name"), rs.getLong("number"));
            } catch (SQLException e) {
                throw new RuntimeSQLException(e);
            }
        }).toList();


    }

    public static List<String> fetchStreamNoTryWithResourceStrings(Connection connection, Statement statement, ResultSet resultSet){

        return JdbcStreamProvider.getStream(connection, statement, resultSet, rs -> {
            try {
                return new TestRecord(rs.getString("name"), rs.getLong("number"));
            } catch (SQLException e) {
                throw new RuntimeSQLException(e);
            }
        }).map(record -> String.format("%s:%s", record.name(), record.number())).toList();


    }

    public static List<String> fetchStreamNoTryWithResourceStringsIterated(Connection connection, Statement statement, ResultSet resultSet){

        var it = JdbcStreamProvider.getStream(connection, statement, resultSet, rs -> {
            try {
                return new TestRecord(rs.getString("name"), rs.getLong("number"));
            } catch (SQLException e) {
                throw new RuntimeSQLException(e);
            }
        }).map(record -> String.format("%s:%s", record.name(), record.number())).iterator();
        List<String> returnList = new LinkedList<>();
        while(it.hasNext())
        {
            returnList.add(it.next());
        }
        return returnList;
    }
}
