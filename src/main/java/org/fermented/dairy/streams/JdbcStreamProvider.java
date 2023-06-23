package org.fermented.dairy.streams;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Spliterators;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class JdbcStreamProvider {

    public static <T> Stream<T> getStream(final Connection connection,
                                          final Statement statement,
                                          final ResultSet resultSet,
                                          final Function<ResultSet, T> mapper) {
        return getStream(
                    connection,
                    statement,
                    resultSet,
                    mapper,
                    lConnection -> {}, //nop
                    lStatement -> {}, //nop
                    lResultSet -> {} //nop
                );

    }

    public static <T> Stream<T> getStream(final Connection connection,
                                          final Statement statement,
                                          final ResultSet resultSet,
                                          final Function<ResultSet, T> mapper,
                                          final Consumer<Connection> connectionPreCloseAction,
                                          final Consumer<Statement> statementPreCloseAction,
                                          final Consumer<ResultSet> resultSetPreCloseAction) {
        return StreamSupport.stream(new Spliterators.AbstractSpliterator<T>(Long.MAX_VALUE, Spliterator.ORDERED) {
                                        @Override
                                        public boolean tryAdvance(Consumer<? super T> consumer) {
                                            try {
                                                if (!resultSet.next()) {
                                                    return false;
                                                }
                                                consumer.accept(mapper.apply(resultSet));
                                                return true;
                                            } catch (SQLException e) {
                                                throw new RuntimeSQLException(e);
                                            }
                                        }
                                    }
                        , false)
                .onClose(() ->
                        {
                            try {

                                if (resultSet != null) {
                                    resultSetPreCloseAction.accept(resultSet);
                                    resultSet.close();
                                }
                                if (statement != null) {
                                    statementPreCloseAction.accept(statement);
                                    statement.close();
                                }
                                if (connection != null) {
                                    connectionPreCloseAction.accept(connection);
                                    connection.close();
                                }
                            } catch (SQLException e) {
                                throw new RuntimeSQLException(e);
                            }
                        }
                );
    }


}
