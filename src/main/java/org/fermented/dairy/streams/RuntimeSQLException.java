package org.fermented.dairy.streams;

import java.sql.SQLException;

public class RuntimeSQLException extends RuntimeException {
    public RuntimeSQLException(SQLException e) {
        super(e);
    }
}
